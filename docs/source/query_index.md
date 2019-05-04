# Query an index

## Usage

    queries - a tool for performing queries on an index.
    Usage: ./bin/queries [OPTIONS]

    Options:
      -h,--help                   Print this help message and exit
      --config TEXT               Configuration .ini file
      -t,--type TEXT REQUIRED     Index type
      -a,--algorithm TEXT REQUIRED
                                  Query algorithm
      -i,--index TEXT REQUIRED    Collection basename
      -w,--wand TEXT              Wand data filename
      -q,--query TEXT             Queries filename
      --compressed-wand           Compressed wand input file
      -k UINT                     k value
      -T,--thresholds TEXT        k value
      --terms TEXT                Text file with terms in separate lines
      --nostem Needs: --terms     Do not stem terms
      --extract                   Extract individual query times
      --silent                    Suppress logging


Now it is possible to query the index.
The command `queries` parses each line of the standard input (or a file if `-q` present)
as a tab-separated collection of term IDs (or words if `--terms` present),
where the i-th term is the i-th list in the input collection.

    $ ./bin/queries -t opt -a and -i test_collection.index.opt -w test_collection.wand -q ../test/test_data/queries

This performs conjunctive queries (`and`). In place of `and` other operators can
be used (`or`, `wand`, ..., see `queries.cpp`), and also multiple operators
separated by colon (`and:or:wand`).

If the WAND file is compressed, please append `--compressed-wand` flag.

## Build additional data

To perform BM25 queries it is necessary to build an additional file containing
the parameters needed to compute the score, such as the document lengths. The
file can be built with the following command:

    $ ./bin/create_wand_data -c ../test/test_data/test_collection -o test_collection.wand

If you want to compress the file append `--compress` at the end of the command.
When using variable-sized blocks (for VBMW) via the `--variable-block` parameter,
you can also specify lambda with the `-l <float>` or `--lambda <float>` flags. 
The value of lambda impacts the mean size of the variable blocks that are
output. See the VBMW paper (listed below) for more details. If using fixed-sized
blocks, which is the default, you can supply the desired block size using the
`-b <UINT> ` or `--block-size <UINT>` arguments. Note that if using fixed/variable
sized blocks, and the `-l` or `-b` parameters are not set, the default parameters
will be used from the configuration file `configuration.hpp`.


## Query algorithms


### AND


### OR


### MaxScore

> Howard Turtle and James Flood. 1995. Query evaluation: strategies and optimizations. Inf. Process. Manage. 31, 6 (November 1995), 831-850. DOI=http://dx.doi.org/10.1016/0306-4573(95)00020-H

### WAND

> Andrei Z. Broder, David Carmel, Michael Herscovici, Aya Soffer, and Jason Zien. 2003. Efficient query evaluation using a two-level retrieval process. In Proceedings of the twelfth international conference on Information and knowledge management (CIKM '03). ACM, New York, NY, USA, 426-434. DOI: https://doi.org/10.1145/956863.956944

### BlockMax WAND

> Shuai Ding and Torsten Suel. 2011. Faster top-k document retrieval using block-max indexes. In Proceedings of the 34th international ACM SIGIR conference on Research and development in Information Retrieval (SIGIR '11). ACM, New York, NY, USA, 993-1002. DOI=http://dx.doi.org/10.1145/2009916.2010048

### BlockMax MaxScore


### Variable BlockMax WAND

> Antonio Mallia, Giuseppe Ottaviano, Elia Porciani, Nicola Tonellotto, and Rossano Venturini. 2017. Faster BlockMax WAND with Variable-sized Blocks. In Proceedings of the 40th International ACM SIGIR Conference on Research and Development in Information Retrieval (SIGIR '17). ACM, New York, NY, USA, 625-634. DOI: https://doi.org/10.1145/3077136.3080780

