Parsing
=======

A _forward index_ is a data structure that stores the term identifiers
associated to every document. Conversely, an _inverted index_ stores for
each unique term the document identifiers where it appears (usually,
associated to a numeric value used for ranking purposes such as the raw
frequency of the term within the document).

The objective of the parsing process is to represent a given collection
as a forward index. To parse a collection, use the `parse_collection`
command:

    parse_collection - parse collection and store as forward index.
    Usage: parse_collection [OPTIONS] [SUBCOMMAND]

    Options:
      -h,--help                   Print this help message and exit
      -L,--log-level TEXT:{critical,debug,err,info,off,trace,warn}=info
                                  Log level
      -j,--threads UINT           Number of threads
      --tokenizer TEXT:{english,whitespace}=english
                                  Tokenizer
      -H,--html UINT=0            Strip HTML
      -F,--token-filters TEXT:{krovetz,lowercase,porter2} ...
                                  Token filters
      --stopwords TEXT            Path to file containing a list of stop words to filter out
      --config TEXT               Configuration .ini file
      -o,--output TEXT REQUIRED   Forward index filename
      -b,--batch-size INT=100000  Number of documents to process in one thread
      -f,--format TEXT=plaintext  Input format

    Subcommands:
      merge                       Merge previously produced batch files. When parsing process was killed during merging, use this command to finish merging without having to restart building batches.

For example:

    $ mkdir -p path/to/forward
    $ zcat ClueWeb09B/*/*.warc.gz | \   # pass unzipped stream in WARC format
        parse_collection \
        -j 8 \                          # use up to 8 threads at a time
        -b 10000 \                      # one thread builds up to 10k documents in memory
        -f warc \                       # use WARC
        -F lowercase porter2 \          # lowercase and stem every term (using the Porter2 algorithm)
        --html \                        # strip HTML markup before extracting tokens
        -o path/to/forward/cw09b

In case you get the error `-bash: /bin/zcat: Argument list too long`,
you can pass the unzipped stream using:

    $ find ClueWeb09B -name '*.warc.gz' -exec zcat -q {} \;

The parsing process will write the following files:
* `cw09b`: forward index in binary format.
* `cw09b.terms`: a new-line-delimited list of sorted terms, where term
  having ID N is on line N, with N starting from 0.
* `cw09b.termlex`: a binary representation (lexicon) of the `.terms`
  file that is used to look up term identifiers at query time.
* `cw09b.documents`: a new-line-delimited list of document titles (e.g.,
  TREC-IDs), where document having ID N is on line N, with N starting
  from 0.
* `cw09b.doclex`: a binary representation of the `.documents` file that
  is used to look up document identifiers at query time.
* `cw09b.urls`: a new-line-delimited list of URLs, where URL having ID N
  is on line N, with N starting from 0. Also, keep in mind that each ID
  corresponds with an ID of the `cw09b.documents` file.

### Generating mapping files

Once the forward index has been generated, a binary document map and
lexicon file will be automatically built. However, they can also be
built using the `lexicon` utility by providing the new-line delimited
file as input. The `lexicon` utility also allows efficient look-ups and
dumping of these binary mapping files.

Examples of the `lexicon` command are shown below:

    Build, print, or query lexicon
    Usage: lexicon [OPTIONS] SUBCOMMAND

    Options:
      -h,--help                   Print this help message and exit
      -L,--log-level TEXT:{critical,debug,err,info,off,trace,warn}=info
                                  Log level
      --config TEXT               Configuration .ini file

    Subcommands:
      build                       Build a lexicon
      lookup                      Retrieve the payload at index
      rlookup                     Retrieve the index of payload
      print                       Print elements line by line

For example, assume we have the following plaintext, new-line delimited
file, `example.terms`:

        aaa
        bbb
        def
        zzz

We can generate a lexicon as follows:

    ./bin/lexicon build example.terms example.lex

You can dump the binary lexicon back to a plaintext representation:

    ./bin/lexicon print example.lex

It should output:

        aaa
        bbb
        def
        zzz

You can retrieve the term with a given identifier:

    ./bin/lexicon lookup example.lex 2

Which outputs:

    def

Finally, you can retrieve the id of a given term:

    ./bin/lexicon rlookup example.lex def

It outputs:

    2

_NOTE_: This requires the initial file to be lexicographically sorted,
as `rlookup` uses binary search for reverse lookups.

### Supported stemmers

* [Porter2](https://snowballstem.org/algorithms/english/stemmer.html)
* [Krovetz](https://dl.acm.org/doi/abs/10.1145/160688.160718)

Both are English stemmers. Unfortunately, PISA does not have support for
any other languages. Contributions are welcome.

### Supported formats

The following raw collection formats are supported:

* `plaintext`: every line contains the document's title first, then any
  number of whitespaces, followed by the content delimited by a new line
  character.
* `trectext`: TREC newswire collections.
* `trecweb`: TREC web collections.
* `warc`: Web ARChive format as defined in [the format
  specification](https://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.0/).
* `wapo`: TREC Washington Post Corpus.

In case you want to parse a set of files where each one is a document (for example, the collection
[wiki-large](http://dg3rtljvitrle.cloudfront.net/wiki-large.tar.gz)), use the `files2trec.py` script
to format it to TREC (take into account that each relative file path is used as the document ID).
Once the file is generated, parse it with the `parse_collection` command specifying the `trectext`
value for the `--format` option.
