'''
=============================================================================
 Copyright 2012 Matt Chaput

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
=============================================================================

Baker lets you easily add a command line interface to your Python functions
using a simple decorator, to create scripts with "sub-commands", similar to
Django's manage.py, svn, hg, etc.
'''

import re
import io
import os
import sys
import gzip
import bz2
from collections import namedtuple
from inspect import getargspec
from textwrap import wrap


__version__ = '1.3'

if sys.version_info[:2] < (3, 0):  # pragma: no cover
    range = xrange


# Stores metadata about a command
Cmd = namedtuple("Cmd", ["name", "fn", "argnames", "keywords", "shortopts",
                         "has_varargs", "has_kwargs", "docstring",
                         "varargs_name", "paramdocs", "is_method"])

PARAM_RE = re.compile(r"^([\t ]*):param (.*?): ([^\n]*\n(\1[ \t]+[^\n]*\n)*)",
                      re.MULTILINE)


def normalize_docstring(docstring):
    """
    Normalizes whitespace in the given string.

        >>> normalize_docstring('This     is\ta docstring.')
        'This is a docstring.'
    """
    return re.sub(r"[\r\n\t ]+", " ", docstring).strip()


def find_param_docs(docstring):
    """
    Finds ReStructuredText-style ":param:" lines in the docstring and
    returns a dictionary mapping param names to doc strings.
    """
    paramdocs = {}
    for match in PARAM_RE.finditer(docstring):
        name = match.group(2)
        value = match.group(3)
        paramdocs[name] = value
    return paramdocs


def remove_param_docs(docstring):
    """
    Finds ReStructuredText-style ":param:" lines in the docstring and
    returns a new string with the param documentation removed.
    """
    return PARAM_RE.sub("", docstring)


def process_docstring(docstring):
    """
    Takes a docstring and returns a list of strings representing
    the paragraphs in the docstring.
    """
    lines = docstring.split("\n")
    paras = [[]]
    for line in lines:
        if not line.strip():
            paras.append([])
        else:
            paras[-1].append(line)
    paras = [normalize_docstring(" ".join(ls)) for ls in paras if ls]
    return paras


def format_paras(paras, width, indent=0, lstripline=None):
    """
    Takes a list of paragraph strings and formats them into a word-wrapped,
    optionally indented string.
    """
    output = []
    for para in paras:
        lines = wrap(para, width - indent)
        if lines:
            for line in lines:
                output.append((" " * indent) + line)
    for i in (lstripline or []):
        output[i] = output[i].lstrip()
    return output


def totype(v, default):
    """
    Tries to convert the value 'v' into the same type as 'default'.

        >>> totype('24', 0)
        24
        >>> totype('True', False)
        True
        >>> totype('0', False)
        False
        >>> totype('1', [])
        '1'
        >>> totype('none', True)  #doctest: +ELLIPSIS
        Traceback (most recent call last):
        ...
        TypeError
    """
    if isinstance(default, bool):
        lv = v.lower()
        if lv in ("true", "yes", "on", "1"):
            return True
        elif lv in ("false", "no", "off", "0"):
            return False
        raise TypeError
    elif isinstance(default, int):
        return int(v)
    elif isinstance(default, float):
        return float(v)
    return v


def openinput(filein):
    """
    Opens the given input file. It can decode various formats too, such as
    gzip and bz2.
    """
    if filein == '-':
        return sys.stdin
    ext = os.path.splitext(filein)[1]
    if ext in ['.gz', '.GZ']:
        return gzip.open(filein, 'rb')
    if ext in ['.bz', '.bz2']:
        return bz2.BZ2File(filein, 'rb')
    return open(filein, 'rb')


class CommandError(Exception):
    """
    General exception for Baker errors, usually related to parsing the
    command line.
    """
    def __init__(self, msg, scriptname, cmd=None):
        super(CommandError, self).__init__(msg)
        self.scriptname = scriptname
        self.commandname = cmd


class TopHelp(Exception):
    """
    Exception raised by Baker.parse() to indicate the user requested the
    overall help for the script, e.g. by typing "script.py help" or
    "script.py --help"
    """
    def __init__(self, scriptname):
        super(TopHelp, self).__init__()
        self.scriptname = scriptname


class CommandHelp(Exception):
    """
    Exception raised by baker.parse() to indicate the user requested help
    for a specific command, e.g. by typing "script.py command --help" or
    "script.py help command".
    """
    def __init__(self, scriptname, cmd):
        super(CommandHelp, self).__init__()
        self.scriptname = scriptname
        self.cmd = cmd


class Baker(object):
    """
    The main class that does all the hard work. It can parse the args and
    format them accordingly.
    """

    def __init__(self, global_options=None):
        self.commands = {}
        self.defaultcommand = None
        self.globalcommand = None
        self.global_options = {}

    def get(self, key, default=None):
        """Shortcut for ``self.global_options.get()``.

        :param key: The dictionary key.
        :param default: The value to return when `key` is not present in
            the dictionary. Otherwise, raise `KeyError`.

        Example. This code::

            >>> b = Baker()
            >>> b.get('opt', 'fallback')

        is the same as this one::

            >>> b = Baker()
            >>> b.global_options.get('opt', 'fallback')
        """
        return self.global_options.get(key, default)

    def command(self, fn=None, name=None, default=False,
                params=None, shortopts=None, global_command=False):
        """
        Registers a command with the bakery. This does not call the
        function, it simply adds it to the list of functions this Baker
        knows about.

        This method is usually used as a decorator::

            b = Baker()

            @b.command
            def test():
                pass

        :param fn: the function to register.
        :param name: use this argument to register the command under a
            different name than the function name.
        :param default: if True, this command is used when a command is not
            specified on the command line.
        :param params: a dictionary mapping parameter names to docstrings. If
            you don't specify this argument, parameter annotations will be used
            (Python 3.x only), or the functions docstring will be searched for
            Sphinx-style ':param' blocks.
        :param shortopts: a dictionary mapping parameter names to short
            options, e.g. {"verbose": "v"}.
        """
        # This method works as a decorator with or without arguments.
        if fn is None:
            # The decorator was given arguments, e.g. @command(default=True),
            # so we have to return a function that will wrap the function when
            # the decorator is applied.
            return lambda fn: self.command(fn, default=default,
                                           name=name,
                                           params=params,
                                           shortopts=shortopts,
                                           global_command=global_command)
        else:
            name = name or fn.__name__

            # Inspect the argument signature of the function
            arglist, vargsname, kwargsname, defaults = getargspec(fn)
            has_varargs = bool(vargsname)
            has_kwargs = bool(kwargsname)
            varargs_name = vargsname

            # Get the function's docstring
            docstring = fn.__doc__ or ""

            # If the user didn't specify parameter help in the decorator
            # arguments, try to get it from parameter annotations (Python 3.x)
            # or RST-style :param: lines in the docstring
            if params is None:
                if hasattr(fn, "func_annotations") and fn.func_annotations:  # pragma: no cover
                    params = fn.func_annotations
                else:
                    params = find_param_docs(docstring)
                    docstring = remove_param_docs(docstring)

            # If the user didn't specify
            shortopts = shortopts or {}
            # Automatically add single letter arguments as shortopts
            shortopts.update(((arg, arg) for arg in arglist if len(arg) == 1))

            # Zip up the keyword argument names with their defaults
            if defaults:
                keywords = dict(zip(arglist[0 - len(defaults):], defaults))
            else:
                keywords = {}

            # If this is a method, remove 'self' from the argument list
            is_method = False
            if arglist and arglist[0] == "self":
                is_method = True
                arglist.pop(0)

            # Create a Cmd object to represent this command and store it
            cmd = Cmd(name, fn, arglist, keywords, shortopts, has_varargs,
                      has_kwargs, docstring, varargs_name, params, is_method)
            # If global_command is True, set this as the global command
            if global_command:
                if defaults is not None and len(defaults) != len(arglist):
                    raise CommandError("Global commands cannot have "
                                       "non-keywords arguments", None)
                if self.defaultcommand is not None:
                    raise CommandError("Default command is already set, you "
                                       "cannot have both", None)
                self.globalcommand = cmd
                self.global_options = keywords
            else:
                self.commands[name] = cmd

            # If default is True, set this as the default command
            if default:
                if self.globalcommand:
                    raise CommandError("Global command is already set, you "
                                       "cannot have both", None)
                self.defaultcommand = cmd

            return fn

    def usage(self, cmd=None, scriptname=None,
              exception=None, fobj=sys.stdout):
        """
        Prints usage of the specified command.
        """
        if exception is not None:
            scriptname, cmd = exception.scriptname, exception.cmd

        if scriptname is None:
            scriptname = sys.argv[0]

        if cmd is None:
            self.print_top_help(scriptname, fobj=fobj)
        else:
            if isinstance(cmd, str):
                cmd = self.commands[cmd]

            self.print_command_help(scriptname, cmd, fobj=fobj)

    def writeconfig(self, iniconffile=sys.argv[0] + ".ini"):
        """
        OVERWRITE an ini style config file that holds all of the default
        command line options.

        :param iniconffile: the file name of the ini file, defaults to
                            '{scriptname}.ini'.
        """
        ret = []
        for cmdname, cmd in self.commands.items():
            ret.append("[%s]" % (cmdname))
            for line in self.return_cmd_doc(cmd):
                ret.append(("# " + line).rstrip())
            for line in self.return_argnames_doc(cmd):
                ret.append(("# " + line).rstrip())
            for key in cmd.keywords:
                head = self.return_head(cmd, key)
                for line in self.return_individual_keyword_doc(cmd, key, head):
                    ret.append(("# " + line).rstrip())
                ret.append("%s = %s\n" % (key, cmd.keywords[key]))
        with open(iniconffile, 'w+') as fp:
            self.write(fp, "\n".join(ret), False)

    @staticmethod
    def write(fobj, content, convert=True):
        """
        Utility function used to write content to a file. Allow compatibility
        between Python versions.
        """

        # This function automatically converts strings to bytes
        # if running under Python 3. Otherwise we cannot write
        # to a file.

        # First detect whether fobj requires binary stream
        if hasattr(fobj, 'mode'):
            # A file-like object
            binary = 'b' in fobj.mode
        else:
            # A subclass of io.BufferedIOBase?
            binary = isinstance(fobj, io.BufferedIOBase)
        # If we are running under Python 3 and binary is required
        if sys.version_info[:2] >= (3, 0) and convert and binary:  # pragma: no cover
            content = bytes(content, 'utf-8')

        fobj.write(content)

    def print_top_help(self, scriptname, fobj=sys.stdout):
        """
        Prints the documentation for the script and exits.

        :param scriptname: the name of the script being executed (argv[0]).
        :param fobj: the file to write the help to. The default is stdout.
        """
        # show global command usage if have one
        if self.globalcommand:
            self.write(fobj, "\n".join(self.return_cmd_doc(self.globalcommand)))
            self.write(fobj, "\n".join(self.return_argnames_doc(self.globalcommand)))
            self.write(fobj, "\n".join(self.return_keyword_doc(self.globalcommand)))
            self.write(fobj, "\n")
 
        # Print the basic help for running a command
        self.write(fobj, "Usage: %s COMMAND <options>\n\n" % scriptname)

        # Get a sorted list of all command names
        cmdnames = sorted(self.commands.keys())
        if cmdnames:
            # Calculate the indent for the doc strings by taking the longest
            # command name and adding 3 (one space before the name and two
            # after)
            rindent = max(len(name) for name in cmdnames) + 3

            self.write(fobj, "Available commands:\n")
            for cmdname in cmdnames:
                # Get the Cmd object for this command
                cmd = self.commands[cmdname]

                self.write(fobj, " " + cmdname)

                # Get the paragraphs of the command's docstring
                paras = process_docstring(cmd.docstring)
                if paras:
                    # Calculate the padding necessary to fill from the end of the
                    # command name to the documentation margin
                    tab = " " * (rindent - len(cmdname) - 1)
                    self.write(fobj, tab)

                    # Print the first paragraph
                    self.write(fobj, "\n".join(format_paras([paras[0]], 76,
                                               indent=rindent,
                                               lstripline=[0])))
                    self.write(fobj, "\n")
                else:
                    self.write(fobj, "\n")

        self.write(fobj, "\n")
        self.write(fobj, "Use '%s <command> --help' for individual command "
                   "help.\n" % scriptname)

    def return_cmd_doc(self, cmd):
        """
        Print the documentation for this command.
        """
        paras = process_docstring(cmd.docstring)
        ret = []
        if paras:
            # Print the first paragraph with no indent (usually just a summary
            # line)
            for line in format_paras([paras[0]], 76):
                ret.append(line)

            # Print subsequent paragraphs indented by 4 spaces
            if len(paras) > 1:
                ret.append("")
                for line in format_paras(paras[1:], 76, indent=4):
                    ret.append(line)
            ret.append("")
        return ret

    def return_argnames_doc(self, cmd):
        """
        Return documentation for required arguments.
        """
        ret = []
        posargs = [a for a in cmd.argnames if a not in cmd.keywords]
        if posargs:
            ret.extend(("", "Required Arguments:", ""))

            # Find the length of the longest formatted heading
            rindent = max(len(argname) + 3 for argname in posargs)
            # Pad the headings so they're all as long as the longest one
            heads = [(head, head.ljust(rindent)) for head in posargs]

            # Print the arg docs
            for keyname, head in heads:
                ret += self.return_individual_keyword_doc(cmd, keyname, head,
                                                          rindent=rindent)
        ret.append("")
        return ret

    def return_individual_keyword_doc(self, cmd, keyname, head, rindent=None):
        """
        Return documentation for optional arguments.
        """
        ret = []
        if rindent is None:
            rindent = len(head) + 2
        if keyname in cmd.paramdocs:
            paras = process_docstring(cmd.paramdocs.get(keyname, ""))
            formatted = format_paras(paras, 76, indent=rindent, lstripline=[0])
            ret.append("  " + head + formatted[0])
            for line in formatted[1:]:
                ret.append("  " + line)
        else:
            ret.append("  " + head.rstrip())
        return ret

    def return_head(self, cmd, keyname):
        """
        Returns the heading of the given command.
        """
        head = " --" + keyname
        if keyname in cmd.shortopts:
            head = " -" + cmd.shortopts[keyname] + head
        head += "  "
        return head

    def return_keyword_doc(self, cmd):
        """
        Return documentation for optional arguments.
        """
        ret = []
        if cmd.keywords:
            ret.append("")
            ret.append("Options:")
            ret.append("")

            # Get a list of keyword argument names
            # cmd.argnames has arguments and keywords in order
            keynames = [a for a in cmd.argnames if a in cmd.keywords]

            # Make formatted headings, e.g. " -k --keyword  ", and put them in
            # a list like [(name, heading), ...]
            heads = [(keyname, self.return_head(cmd, keyname)) for keyname in
                    keynames]

            rindent = None
            if heads:
                # Find the length of the longest formatted heading
                rindent = max(len(head) + 2 for keyname, head in heads)
                # Pad the headings so they're all as long as the longest one
                heads = [(keyname, head + (" " * (rindent - len(head) - 2)))
                         for keyname, head in heads]

                # Print the option docs
                for keyname, head in heads:
                    ret += self.return_individual_keyword_doc(cmd, keyname,
                                                              head, rindent -
                                                              2)

        if cmd.has_varargs:
            ret.extend(("", "Variable arguments:", ""))
            keyname = cmd.varargs_name
            head = " *%s   " % (keyname)
            ret += self.return_individual_keyword_doc(cmd, keyname, head,
                    len(head))

        ret.append("")
        if any(cmd.keywords.get(a) is None for a in cmd.argnames):
            ret.append("(specifying a double hyphen (--) in the argument"
                       " list means all")
            ret.append("subsequent arguments are treated as bare "
                       "arguments, not options)")
            ret.append("")
        return ret

    def print_command_help(self, scriptname, cmd, fobj=sys.stdout):
        """
        Prints the documentation for a specific command and exits.

        :param scriptname: the name of the script being executed (argv[0]).
        :param cmd: the Cmd object representing the command.
        :param fobj: the file to write the help to. The default is stdout.
        """

        # Print the usage for the command
        self.write(fobj, "Usage: %s %s" % (scriptname, cmd.name))

        # Print the required and "optional" arguments (where optional
        # arguments are keyword arguments with default None).
        for name in cmd.argnames:
            if name not in cmd.keywords:
                # This is a positional argument
                self.write(fobj, " <%s>" % name)
            else:
                # This is a keyword/optional argument
                self.write(fobj, " [<%s>]" % name)

        if cmd.has_varargs:
            # This command accepts a variable number of positional arguments
            self.write(fobj, " [<%s>...]" % (cmd.varargs_name))
        self.write(fobj, "\n\n")

        self.write(fobj, "\n".join(self.return_cmd_doc(cmd)))
        self.write(fobj, "\n".join(self.return_argnames_doc(cmd)))
        self.write(fobj, "\n".join(self.return_keyword_doc(cmd)))
        if self.globalcommand is not None:
            self.write(fobj, "\n".join(self.return_cmd_doc(cmd)))

    def parse_args(self, scriptname, cmd, argv, test=False):
        """
        Parse arguments from argv.

        :param scriptname: The script filename.
        :param cmd: The command which is being called.
        :param argv: The argument list.
        :param test: If True prints to stdout.
        """
        keywords = cmd.keywords
        shortopts = cmd.shortopts

        def type_error(name, value, t):
            if not test:
                msg = "%s value %r must be %s" % (name, value, t)
                raise CommandError(msg, scriptname, cmd)

        # shortopts maps long option names to characters. To look up short
        # options, we need to create a reverse mapping.
        shortchars = dict((v, k) for k, v in shortopts.items())

        # The *args list and **kwargs dict to build up from the command line
        # arguments
        vargs = []
        kwargs = {}

        double_dash = 0
        single_dash = 0
        while argv:
            # Take the next argument
            arg = argv.pop(0)

            if arg == "--":
                double_dash += 1
                if double_dash != 1:
                    raise CommandError("You cannot specify -- more than once")
                # All arguments following a double hyphen are treated as
                # positional arguments
                vargs.extend(argv)
                break

            elif arg == "-":
                # sys.stdin
                single_dash += 1
                if single_dash != 1:
                    raise CommandError("You cannot specify - more than once")
                vargs.append("-")

            elif arg.startswith("--"):
                # Process long option

                value = None
                if "=" in arg:
                    # The argument was specified like --keyword=value
                    name, value = arg[2:].split("=", 1)
                    # strip quotes if value is quoted like
                    # --keyword='multiple words'
                    value = value.strip('\'"')

                    default = keywords.get(name)
                    try:
                        value = totype(value, default)
                    except (TypeError, ValueError):
                        type_error(name, value, type(default))
                else:
                    # The argument was not specified with an equals sign...
                    name = arg[2:]
                    default = keywords.get(name)

                    if type(default) is bool:
                        # If this option is a boolean, it doesn't need a value;
                        # specifying it on the command line means "do the
                        # opposite of the default".
                        value = not default
                    else:
                        # The next item in the argument list is the value, i.e.
                        # --keyword value
                        if not argv or argv[0].startswith("-"):
                            # Oops, there isn't a value available... just use
                            # True, assuming this is a flag.
                            value = True
                        else:
                            value = argv.pop(0)

                        try:
                            value = totype(value, default)
                        except (TypeError, ValueError):
                            type_error(name, value, type(default))

                # Store this option
                kwargs[name] = value

            elif arg.startswith("-") and (cmd.shortopts or cmd.has_kwargs):
                # Process short option(s)

                # For each character after the '-'...
                for i in range(1, len(arg)):
                    char = arg[i]
                    if cmd.has_kwargs:
                        name = char
                        default = keywords.get(name)
                    elif char not in shortchars:
                        continue
                    else:
                        # Get the long option name corresponding to this char
                        name = shortchars[char]
                        default = keywords[name]

                    if isinstance(default, bool):
                        # If this option is a boolean, it doesn't need a value;
                        # specifying it on the command line means "do the
                        # opposite of the default".
                        kwargs[name] = not default
                    else:
                        # This option requires a value...
                        if i == len(arg) - 1:
                            # This is the last character in the list, so the
                            # next argument on the command line is the value.
                            value = argv.pop(0)
                        else:
                            # There are other characters after this one, so
                            # the rest of the characters must represent the
                            # value (i.e. old-style UNIX option like -Nname)
                            value = arg[i + 1:]

                        # Remove leading equals sign if it's present. That
                        # means the option/value were specified as opt=value
                        # Then remove quotes.
                        value = value.lstrip("=").strip("'\"")

                        try:
                            kwargs[name] = totype(value, default)
                        except (TypeError, ValueError):
                            type_error(name, value, type(default))
                        break
            else:
                # This doesn't start with "-", so just add it to the list of
                # positional arguments.
                vargs.append(arg)

        return vargs, kwargs

    def parse(self, argv=None, test=False):
        """
        Parses the command and parameters to call from the list of command
        line arguments. Returns a tuple of (scriptname string, Cmd object,
        position arg list, keyword arg dict).

        This method will raise TopHelp if the parser finds that the user
        requested the overall script help, and raise CommandHelp if the user
        requested help on a specific command.

        :param argv: the list of options passed to the command line (sys.argv).
        """

        if argv is None:
            argv = sys.argv

        scriptname = argv[0]
        argv_len = len(argv)

        if argv_len < 2 and self.defaultcommand is None:
            raise TopHelp(scriptname)

        if argv_len > 1:
            if argv[1] in ("-h", "--help"):
                # Print the documentation for the script
                raise TopHelp(scriptname)

            elif argv[1] == "help":
                if argv_len > 2 and argv[2] in self.commands:
                    cmd = self.commands[argv[2]]
                    raise CommandHelp(scriptname, cmd)
                raise TopHelp(scriptname)

        if argv_len > 1 and argv[1] in self.commands:
            # The first argument on the command line (after the script name
            # is the command to run.
            cmd = self.commands[argv[1]]

            if argv_len > 2 and (argv[2] == "-h" or argv[2] == "--help"):
                raise CommandHelp(scriptname, cmd)

            options = argv[2:]
        elif self.defaultcommand is not None:
            # No known command was specified. If there's a default command,
            # use that.
            cmd = self.defaultcommand
            options = argv[1:]
        elif self.globalcommand is not None:
            # First, get the name of the real command.
            # It cannot be after an option (--opt) except when that option
            # is a boolean (i.e. does not have a value).
            for i, arg in enumerate(argv[1:], 1):
                prev = argv[i - 1]
                is_prev_bool = isinstance(self.get(prev.lstrip('-')), bool)
                candidate = not prev.startswith('-') or is_prev_bool
                if arg in self.commands and candidate:
                    break
            else:
                raise CommandError("No command specified", scriptname)
            global_args, options = argv[1:i], argv[i + 1:]
            args, kwargs = self.parse_args(scriptname, self.globalcommand,
                                           global_args, test=test)
            self.global_options = self.apply(scriptname, self.globalcommand,
                                             args, kwargs)
            cmd = self.commands[argv[i]]
        else:
            raise CommandError("No command specified", scriptname)

        # Parse the rest of the arguments on the command line and use them to
        # call the command function.
        args, kwargs = self.parse_args(scriptname, cmd, options, test=test)
        return (scriptname, cmd, args, kwargs)

    def apply(self, scriptname, cmd, args, kwargs, instance=None):
        """
        Calls the command function.
        """

        # Create a list of positional arguments: arguments that are either
        # required (not in keywords), or where the default is None (taken to be
        # an optional positional argument). This is different from the Python
        # calling convention, which will fill in keyword arguments with extra
        # positional arguments.

        # Rearrange the arguments into the order Python expects
        newargs = []
        newkwargs = kwargs.copy()
        for name in cmd.argnames:
            if name in cmd.keywords:
                if not args:
                    break
                # keyword arg
                if cmd.has_varargs:
                    # keyword params are not replaced by bare args if the func
                    # also has varags but they must be specified as positional
                    # args for proper processing of varargs
                    value = cmd.keywords[name]
                    if name in newkwargs:
                        value = newkwargs[name]
                        del newkwargs[name]
                    newargs.append(value)
                elif not name in newkwargs:
                    newkwargs[name] = args.pop(0)

            else:
                # positional arg
                if name in newkwargs:
                    newargs.append(newkwargs[name])
                    del newkwargs[name]
                else:
                    if args:
                        newargs.append(args.pop(0))
                    else:
                        # This argument is required but we don't have a bare
                        # arg to fill it
                        msg = "Required argument %r not given"
                        raise CommandError(msg % (name), scriptname, cmd)
        if args:
            if cmd.has_varargs:
                newargs.extend(args)
            else:
                msg = "Too many arguments to %r: %s"
                raise CommandError(msg % (cmd.name, args), scriptname, cmd)

        if not cmd.has_kwargs:
            for k in newkwargs:
                if k not in cmd.keywords:
                    raise CommandError("Unknown option --%s" % k,
                                       scriptname, cmd)

        if cmd.is_method and instance is not None:
            return cmd.fn(instance, *newargs, **newkwargs)
        return cmd.fn(*newargs, **newkwargs)

    def run(self, argv=None, main=True, help_on_error=False,
            outfile=sys.stdout, errorfile=sys.stderr, helpfile=sys.stdout,
            errorcode=1, instance=None):
        """
        Takes a list of command line arguments, parses it into a command
        name and options, and calls the function corresponding to the command
        with the given arguments.

        :param argv: the list of options passed to the command line (sys.argv).
        :param main: if True, print error messages and exit instead of
            raising an exception.
        :param help_on_error: if True, when an error occurs, print the usage
            help after the error.
        :param errorfile: the file to write error messages to.
        :param helpfile: the file to write usage help to.
        :param errorcode: the exit code to use when calling sys.exit() in the
            case of an error. If this is 0, sys.exit() will not be called.
        """

        try:
            value = self.apply(*self.parse(argv), instance=instance)
            if main and value is not None:
                self.write(outfile, str(value) + '\n')
            return value
        except TopHelp as e:
            if not main:
                raise
            self.usage(scriptname=e.scriptname, fobj=helpfile)
        except CommandHelp as e:
            if not main:
                raise
            self.usage(e.cmd, scriptname=e.scriptname, fobj=helpfile)
        except CommandError as e:
            if not main:
                raise
            self.write(errorfile, str(e) + "\n")
            if help_on_error:
                self.write(errorfile, "\n")
                self.usage(e.cmd, scriptname=e.scriptname, fobj=helpfile)
            if errorcode:
                sys.exit(errorcode)

    def test(self, argv=None, fobj=sys.stdout):
        """
        Takes a list of command line arguments, parses it into a command
        name and options, and prints what the resulting function call would
        look like. This may be useful for testing how command line arguments
        would be passed to your functions.

        :param argv: the list of options passed to the command line (sys.argv).
        :param fobj: the file to write result to.
        """

        try:
            _, cmd, args, kwargs = self.parse(argv, test=True)
            result = "%s(%s" % (cmd.name, ", ".join(repr(a) for a in args))
            if kwargs:
                kws = ", ".join("%s=%r" % (k, v) for k, v in kwargs.items())
                result += ", " + kws
            result += ")"
            self.write(fobj, result)
            return result  # useful for testing
        except TopHelp:
            self.write(fobj, "(top-level help)")
        except CommandHelp as e:
            self.write(fobj, "(help for %s command)" % e.cmd.name)


_baker = Baker()
command = _baker.command
run = _baker.run
test = _baker.test
usage = _baker.usage
writeconfig = _baker.writeconfig
