#!/usr/bin/env python

import os
import sys
from subprocess import check_call, call, Popen, PIPE
from time import time, sleep
import logging
import platform
import json
import multiprocessing

sys.path.append('..')
from ext import baker

if platform.system() == 'Darwin':
    TIME_EXE = 'gtime' # requires installation of gnu-time
    NUMACTL = ''
    NUMACTL_LKP = ''
    DROP_CACHES_SUPPORTED = False
else:
    TIME_EXE = '/usr/bin/time'
    # NUMACTL = 'numactl -i all -- '
    # NUMACTL_LKP = 'numactl --preferred 0 -C 2 '
    NUMACTL = ''
    NUMACTL_LKP = ''
    DROP_CACHES_SUPPORTED = True

MAX_THREADS = multiprocessing.cpu_count()

SCRIPTDIR = os.path.abspath(os.path.dirname(__file__) + '/..')
INDEX_TYPES = ['single', 'uniform', 'opt', 'block_interpolative', 'block_optpfor', 'block_varint']

_log = logging.getLogger('ROOT')

def ensure_dir(d):
    check_call(['mkdir', '-p', d])

def drop_caches():
    if not DROP_CACHES_SUPPORTED:
        _log.warning('Dropping caches not supported')
        return

    try:
        ret = call(['%s/build/drop_caches' % SCRIPTDIR])
        if ret:
            _log.warning('Could not drop caches (retcode %s)', ret)
        else:
            sleep(5) # Allow other processes to warm up again
            return True
    except OSError:
        _log.warning("Could not find drop_caches, can't drop caches")
    return False

def load(filename):
    import pandas
    return pandas.read_csv(filename, sep='\t', header=None, names=range(4))

def plot_perftest(fig, filename):
    TYPES = ['block_optpfor', 'block_varint', 'ef', 'single', 'uniform', 'opt']
    df = load(filename)

    fig.suptitle(filename)
    ax1 = fig.add_subplot(1, 2, 1)

    for t in TYPES:
        d = df[(df[0] == t) & (df[1] == 'next_geq')]
        ax1.plot(d[2], d[3], label=t)

    ax1.set_xscale('log', basex=2)
    ax1.legend(loc=2)
    ax1.set_title('next_geq()')

    ax2 = fig.add_subplot(1, 2, 2)

    for t in TYPES:
        d = df[(df[0] == t) & (df[1] == 'next_geq_freq')]
        ax2.plot(d[2], d[3], label=t)

    ax2.set_xscale('log', basex=2)
    ax2.legend(loc=2)
    ax2.set_title('next_geq() + freq()')

    ybound = max(ax1.get_ylim()[1], ax2.get_ylim()[1])
    ax1.set_ylim((0, ybound))
    ax2.set_ylim((0, ybound))


@baker.command
def plot_perftests(*filenames):
    from pylab import figure
    from matplotlib.backends.backend_pdf import PdfPages
    pp = PdfPages('perftests.pdf')

    for filename in filenames:
        fig = figure()
        plot_perftest(fig, filename)
        fig.set_size_inches(18, 6)
        pp.savefig(fig, bbox_inches='tight')

    pp.close()


TOOLS_JAR = 'mg4j-utils-1.0-SNAPSHOT-jar-with-dependencies.jar'
QUERIES_FILES = [
    'trec_queries/05.efficiency_queries',
    'trec_queries/06.efficiency_queries',
]

def java_tool(classname, *args, **kwargs):
    check_call(['java', '-server', '-Xmx16384M', '-XX:-UseGCOverheadLimit',
                '-cp', os.path.join(SCRIPTDIR, TOOLS_JAR),
                classname] + list(args),
               stdin=kwargs.get('stdin'),
               stdout=kwargs.get('stdout'))


@baker.command
def prepare_data(basename):
    java_tool('it.cnr.isti.hpc.mg4jutils.DumpIndex', basename, basename + '.bin')

@baker.command
def prepare_queries(basename):
    scriptdir = SCRIPTDIR

    for idx, qfile in enumerate(QUERIES_FILES):
        mappedqfile = '%s.queries%d.mapped' % (basename, idx)
        with open(os.path.join(SCRIPTDIR, qfile)) as qin, \
             open(mappedqfile, 'w') as qout:
            java_tool('it.cnr.isti.hpc.mg4jutils.QueryMapper', basename + '.termmap',
                      stdin=qin, stdout=qout)

        check_call(('sort -R "%(mappedqfile)s" | head -n 1000 '
                    '> "%(mappedqfile)s.1k"') % locals(),
                   shell=True)

        check_call(('sort -R "%(mappedqfile)s" | grep -E "\s" | head -n 1000 '
                    '> "%(mappedqfile)s.multiterm.1k"') % locals(),
                   shell=True)

        check_call(('%(scriptdir)s/build/selective_queries single %(basename)s.bin.single '
                    '< %(mappedqfile)s.1k '
                    '> %(mappedqfile)s.selective') % locals(),
                   shell=True)


def logfile_cname(basename, logname):
    directory = os.path.join(os.path.dirname(basename), 'results')
    ensure_dir(directory)
    name = '%s.%s' % (os.path.basename(basename), logname)
    return os.path.join(directory, name)


def logfile(basename, logname):
    name = logfile_cname(basename, logname)
    tsname = '%s.%s' % (name, int(time()))
    check_call(['ln', '-sf', os.path.basename(tsname), name])
    return tsname


@baker.command
def build_indexes(basename, fast=False, check=False):
    scriptdir = SCRIPTDIR
    logfilename = logfile(basename, 'build_indexes')

    check_opt = '--check' if check else ''
    env = os.environ.copy()
    env['PISA_THREADS'] = str(MAX_THREADS) if fast else '0'
    for index_type in INDEX_TYPES:
        print >> sys.stderr, '\nBuilding %(index_type)s index' % locals()
        if not fast:
            drop_caches()
        check_call((NUMACTL +
                    '%(scriptdir)s/build/create_freq_index %(index_type)s '
                    '%(basename)s.bin %(basename)s.bin.%(index_type)s '
                    '%(check_opt)s '
                    '>> %(logfilename)s') % locals(),
                   shell=True,
                   env=env)

    if not fast:
        # run opt again at full speed
        env['PISA_THREADS'] = str(MAX_THREADS)
        index_type = 'opt'
        print >> sys.stderr, '\nBuilding %(index_type)s index' % locals()
        drop_caches()
        check_call((NUMACTL +
                    '%(scriptdir)s/build/create_freq_index %(index_type)s '
                    '%(basename)s.bin '
                    '>> %(logfilename)s') % locals(),
                   shell=True,
                   env=env)

    check_call((NUMACTL +
                '%(scriptdir)s/build/create_wand_data '
                '%(basename)s.bin %(basename)s.bin.wand '
                '>> %(logfilename)s') % locals(),
               shell=True)


@baker.command
def uniform_part_sizes(basename):
    scriptdir = SCRIPTDIR

    env = os.environ.copy()
    env['PISA_THREADS'] = '1'
    logfilename = logfile(basename, 'uniform_part_sizes')
    for l in xrange(5, 11):
        env['PISA_LOG_PART'] = str(l)
        check_call((NUMACTL +
                    '%(scriptdir)s/build/create_freq_index uniform '
                    '%(basename)s.bin '
                    '>> %(logfilename)s') % locals(),
                   shell=True, env=env)


@baker.command
def sweep_eps(basename):
    scriptdir = SCRIPTDIR

    env = os.environ.copy()
    env['PISA_THREADS'] = MAX_THREADS

    drop_caches()
    # Dry run to ensure data is in page cache
    check_call((NUMACTL +
                '%(scriptdir)s/build/create_freq_index single '
                '%(basename)s.bin '
                '> /dev/null') % locals(),
               shell=True, env=env)

    env['PISA_THREADS'] = 1

    env['PISA_EPS1'] = str(0)
    logfilename = logfile(basename, 'sweep_eps2')
    for l in xrange(25, 525, 25):
        env['PISA_EPS2'] = str(l / 1000.0)
        check_call((NUMACTL +
                    '%(scriptdir)s/build/create_freq_index opt '
                    '%(basename)s.bin '
                    '>> %(logfilename)s') % locals(),
                   shell=True, env=env)

    env['PISA_EPS2'] = str(0.3)
    logfilename = logfile(basename, 'sweep_eps1')
    for l in xrange(0, 105, 5):
        env['PISA_EPS1'] = str(l / 1000.0)
        check_call((NUMACTL +
                    '%(scriptdir)s/build/create_freq_index opt '
                    '%(basename)s.bin '
                    '>> %(logfilename)s') % locals(),
                   shell=True, env=env)


@baker.command
def queries(basename):
    scriptdir = SCRIPTDIR
    for idx in xrange(len(QUERIES_FILES)):
        for variant in ['1k', 'selective']:
            logfilename = logfile(basename, 'queries%d.%s' % (idx, variant))

            queriesfilename = '%(basename)s.queries%(idx)s.mapped.%(variant)s' % locals()
            for index_type in INDEX_TYPES:
                print >> sys.stderr, '\nQuerying %(index_type)s index' % locals()

                drop_caches()
                check_call((NUMACTL_LKP +
                            '%(scriptdir)s/build/queries %(index_type)s '
                            'and:or:ranked_and:wand:maxscore '
                            '%(basename)s.bin.%(index_type)s '
                            '%(basename)s.bin.wand '
                            '< %(queriesfilename)s '
                            '>> %(logfilename)s') % locals(),
                           shell=True)

def read_log(logfile):
    with open(logfile) as fin:
        return [json.loads(line) for line in fin]


def select(log, field=None, **constraints):
    results = []
    for line in log:
        for k, v in constraints.iteritems():
            if k not in line or line[k] != v:
                break
        else:
            if field is not None:
                try:
                    if isinstance(field, str):
                        results.append(line[field])
                    elif isinstance(field, tuple):
                        results.append(tuple(line[f] for f in field))
                    else:
                        results.append(field(line))
                except KeyError:
                    pass
            else:
                results.append(line)

    return results

def print_rows(fout, headers, rows):
    cols = len(headers)
    best = list(rows[0])

    for row in rows:
        if row is None: continue

        for col in xrange(cols):
            if headers[col][1] == '<' and row[col] < best[col]:
                best[col] = row[col]
            if headers[col][1] == '>' and row[col] > best[col]:
                best[col] = row[col]

    threshold = 1.1

    for row in rows:
        if row is None:
            print >> fout, r'\midrule'
            continue

        for col in xrange(cols):
            if col > 0:
                print >> fout, ' & ',

            if row[col] is None: continue

            highlight = False
            if headers[col][1] == '<' and row[col] <= best[col] * threshold:
                highlight = True
            if headers[col][1] == '>' and row[col] >= best[col] / threshold:
                highlight = True

            tmpl = headers[col][0]
            if highlight:
                tmpl = r'\boldmath{%s}' % tmpl

            print >> fout, tmpl % row[col],

        print >> fout, r'\\'


DATASETS = [
    ('gov2.sorted-text', r'\gov'),
    ('clueweb.sorted-text', r'\clue'),
]

INDEX_NAMES = {
    'single': r'\singleInd',
    'uniform': r'\uniformInd',
    'opt': r'\optInd',
    'block_optpfor': r'\optpforInd',
    'block_varint': r'\varintInd',
    'block_interpolative': r'\interpolativeInd',
}

def total_size(line):
    return line['size']

@baker.command
def tables():
    ensure_dir('tables')

    # Space
    with open('tables/space.tex', 'w') as fout:
        logs = [read_log('results/%s.build_indexes' % d) for d, _ in DATASETS]

        for _, dname in DATASETS:
            print >> fout, r' & \multicolumn{6}{@{}c@{}}{%s}' % dname,

        print >> fout, r'\\'

        for i in xrange(len(DATASETS)):
            print >> fout, r'\cmidrule(lr){%d-%d}' % (2 + i * 6, 2 + (i + 1) * 6 - 1),

        print >> fout

        headers = [('%s', None)]
        for _ in DATASETS:
            for col in ['space', 'doc', 'freq']:
                print >> fout, r' & \multicolumn{2}{@{}c@{}}{\textsf{%s}}' % col,
                headers += [('$%.2f$', '='), (r'\scriptsize{\color{DarkGray}$(%+.1f\%%)$}', '=')]

        print >> fout, r'\\'

        for _ in DATASETS:
            for col in ['GB', 'bpi', 'bpi']:
                print >> fout, r' & \multicolumn{2}{@{}c@{}}{\textsf{%s}}' % col,

        print >> fout, r'\\ \midrule'

        rows = []
        for t in INDEX_TYPES:
            row = []
            row.append(INDEX_NAMES[t])

            for log in logs:
                size = select(log, total_size, type=t)[0]
                row.append(size / 10.0**9)
                row.append(None)

                dbpi = select(log, 'bits_per_doc', type=t)[0]
                row.append(dbpi)
                row.append(None)
                fbpi = select(log, 'bits_per_freq', type=t)[0]
                row.append(fbpi)
                row.append(None)

                # ctime = select(log, 'construction_time', type=t, worker_threads=0)[0]
                # row.append(ctime / 60.)

            rows.append(row)
            if t == 'opt':
                rows.append(None)

        # relative percentages
        reference = 2
        for row_idx, row in enumerate(rows):
            if row is None or row_idx == reference: continue
            for col in xrange(1, len(row), 2):
                percentage = (100.0 * row[col] / rows[reference][col]) - 100.0
                row[col + 1] = percentage

        print_rows(fout, headers, rows)


    # # Construction time
    # with open('tables/build_time.tex', 'w') as fout:
    #     logs = [read_log('results/%s.build_indexes' % d) for d, _ in DATASETS]

    #     headers = [('%s', None)]
    #     for _, dname in DATASETS:
    #         print >> fout, r' & \multicolumn{1}{@{}c@{}}{%s}' % dname,
    #         headers.append(('$%.1f$', '='))

    #     print >> fout, r'\\ \midrule'

    #     rows = []
    #     for t in INDEX_TYPES:
    #         row = [INDEX_NAMES[t]]

    #         for log in logs:
    #             ctime = select(log, 'construction_time', type=t, worker_threads=0)[0]
    #             row.append(ctime / 60.)

    #         rows.append(row)
    #         if t == 'opt':
    #             rows.append(None)

    #     # rows.append(None)
    #     # threads = select(logs[0], 'worker_threads', type='opt')[-1]
    #     # row = [INDEX_NAMES['opt'] + r'\ ($%d$ threads)' % threads]
    #     # for log in logs:
    #     #     ctime = select(log, 'construction_time', type='opt', worker_threads=threads)[0]
    #     #     row.append(ctime / 60.)

    #     # rows.append(row)
    #     print_rows(fout, headers, rows)

    all_query_logs = [read_log('results/%s.queries%d.1k' % (d, q))
                      for d, _ in DATASETS for q in xrange(len(QUERIES_FILES))]

    sel_query_logs = [read_log('results/%s.queries%d.selective' % (d, q))
                      for d, _ in DATASETS for q in xrange(len(QUERIES_FILES))]


    def query_table(qtype, fout, logs):
        for _, dname in DATASETS:
            print >> fout, r' & \multicolumn{4}{@{}c@{}}{%s}' % dname,

        print >> fout, r'\\'

        for i in xrange(len(DATASETS)):
            print >> fout, r'\cmidrule(lr){%d-%d}' % (2 + i * 4, 2 + (i + 1) * 4 - 1),

        print >> fout

        headers = [('%s', None)]
        for _ in DATASETS:
            for col in ['TREC 05', 'TREC 06']:
                print >> fout, r' & \multicolumn{2}{@{}c@{}}{\textsf{%s}}' % col,

            headers += [('$%.1f$', '='), (r'\tiny{\color{DarkGray}$(%+.0f\%%)$}', '=')] * 2

        print >> fout, r'\\ \midrule'

        rows = []
        for idx_t, t in enumerate(INDEX_TYPES):
            row = []
            row.append(INDEX_NAMES[t])

            for log in logs:
                avg = (select(log, 'avg', type=t, query=qtype) + [10**6])[0] # XXX
                row.append(avg / 1000)
                row.append(None)

            rows.append(row)
            if t == 'opt':
                rows.append(None)

        # Compute relative percentages
        reference = 2
        for row_idx, row in enumerate(rows):
            if row is None or row_idx == reference: continue
            for col in xrange(1, len(row), 2):
                percentage = (100.0 * row[col] / rows[reference][col]) - 100.0
                row[col + 1] = percentage

        print_rows(fout, headers, rows)


    # Boolean
    with open('tables/or_query.tex', 'w') as fout:
        query_table('or', fout, all_query_logs)

    with open('tables/and_query.tex', 'w') as fout:
        query_table('and', fout, all_query_logs)

    with open('tables/and_query_selective.tex', 'w') as fout:
        query_table('and', fout, sel_query_logs)

    # Ranked
    with open('tables/ranked_and_query.tex', 'w') as fout:
        query_table('ranked_and', fout, all_query_logs)

    with open('tables/ranked_and_query_selective.tex', 'w') as fout:
        query_table('ranked_and', fout, sel_query_logs)

    with open('tables/wand_query.tex', 'w') as fout:
        query_table('wand', fout, all_query_logs)

    with open('tables/wand_query_selective.tex', 'w') as fout:
        query_table('wand', fout, sel_query_logs)

    with open('tables/maxscore_query.tex', 'w') as fout:
        query_table('maxscore', fout, all_query_logs)

    with open('tables/maxscore_query_selective.tex', 'w') as fout:
        query_table('maxscore', fout, sel_query_logs)


def plot_part_sizes(fig, d):
    from matplotlib import ticker

    part_sizes_log = read_log('results/%s.uniform_part_sizes' % d)
    build_indexes_log = read_log('results/%s.build_indexes' % d)

    single_size = select(build_indexes_log, total_size, type='single')[0]
    opt_size = select(build_indexes_log, total_size, type='opt')[0]

    log_part_sizes = select(part_sizes_log, 'log_partition_size')
    total_sizes = select(part_sizes_log, total_size)

    ax = fig.gca()
    marker='.'
    pl_uniform, = ax.plot(log_part_sizes, total_sizes, marker=marker)
    pl_single, = ax.plot(log_part_sizes, [single_size] * len(log_part_sizes), ls=':', marker=marker)
    pl_optimal, = ax.plot(log_part_sizes, [opt_size] * len(log_part_sizes), ls='--', dashes=(2, 2), marker=marker)

    ax.xaxis.set_major_formatter(ticker.FuncFormatter(lambda x, pos: '$%d$' % 2**x))
    ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, pos: '$%.2f$' % (x / 10.**9)))
    ax.yaxis.set_minor_locator(ticker.AutoMinorLocator())

    ax.grid(True, 'major', color='w', linestyle='-', linewidth=0.7)
    ax.grid(True, 'minor', color='0.95', linestyle='-', linewidth=0.2)

    ax.legend([pl_single, pl_uniform, pl_optimal],
              ['EF single', 'EF uniform', 'EF $\epsilon$-opt'],
              prop={'size': 9})
    ax.set_ylim(bottom=0.95 * opt_size)
    ax.set_ylim(top=1.05 * single_size)

    fig.set_size_inches(5, 1.8)


def plot_eps_sweep(fig, d, eps):
    from matplotlib import ticker

    log = read_log('results/%s.sweep_%s' % (d, eps))

    epss = select(log, eps)
    total_sizes = select(log, total_size)
    mt_adj = 0.75 # unbias the estimation given by CPU time
    times = [t / 60. * mt_adj for t in select(log, 'construction_user_time')]
    ax = fig.gca()
    size_color = next(ax._get_lines.color_cycle)
    time_color = next(ax._get_lines.color_cycle)
    marker='.'

    ax.plot(epss, total_sizes, color=size_color, marker=marker)
    # for tl in ax.get_yticklabels():
    #     tl.set_color(size_color)

    ax2 = ax.twinx()
    ax2.plot(epss, times, linestyle='--', color=time_color, marker=marker)
    # for tl in ax2.get_yticklabels():
    #     tl.set_color(time_color)


    ax.set_xlim(0, max(epss))
    ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, pos: '$%.2f$' % (x / 10.**9)))
    ax.yaxis.set_minor_locator(ticker.AutoMinorLocator())

    ax.grid(True, 'major', color='w', linestyle='-', linewidth=0.7)
    ax.grid(True, 'minor', color='0.95', linestyle='-', linewidth=0.2)

    fig.set_size_inches(5, 1.8)

@baker.command
def figures():
    from pylab import figure
    from matplotlib import rc
    rc('ps', useafm=True)
    rc('pdf', use14corefonts=True)
    rc('text', usetex=True)
    rc('font', family='sans-serif')
    rc('font', **{'sans-serif': ['Computer Modern']})

    from mpltools import style
    style.use('ggplot')
    rc('axes', grid=False)

    ensure_dir('figures')

    for d, _ in DATASETS:
        fig = figure()
        plot_part_sizes(fig, d)
        fig.savefig('figures/%s.part_sizes.pdf' % d, bbox_inches='tight')

    for eps in ['eps1', 'eps2']:
        d, _ = DATASETS[0]
        fig = figure()
        plot_eps_sweep(fig, d, eps)
        fig.savefig('figures/%s.sweep_%s.pdf' % (d, eps), bbox_inches='tight')


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s:%(levelname)s: %(message)s')
    baker.run()
