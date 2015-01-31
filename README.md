ds2i
====

Data Structures for Inverted Indexes (ds2i) is a library of data structures to
represent the integer sequences used in inverted indexes.

This code was used in the experiments of the following papers.

* Giuseppe Ottaviano, Rossano Venturini, _Partitioned Elias-Fano Indexes_,
  ACM SIGIR 2014.

* Giuseppe Ottaviano, Nicola Tonellotto, Rossano Venturini, _Optimal Space-Time
  Tradeoffs for Inverted Indexes_, ACM WSDM 2015.


Building the code
-----------------

The code is tested on Linux with GCC 4.9 and OSX Yosemite with Clang.

The following dependencies are needed for the build.

* CMake >= 2.8, for the build system
* Boost >= 1.51

The code depends on several git submodules. If you have cloned the repository
without `--recursive`, you will need to perform the following commands before
building:

    $ git submodule init
    $ git submodule update

To build the code:

    $ mkdir build
    $ cd build
    $ cmake . -DCMAKE_BUILD_TYPE=Release
    $ make

It is also preferable to perform a `make test`, which runs the unit tests.


Example: Partitioned Elias-Fano
-------------------------------

The directory `test/test_data` contains a small document collection used in the
unit tests. The binary format of the collection is described in a following
section.

To create an index use the command `create_freq_index`. The available index
types are listed in `index_types.hpp`. For example, to create an index using the
optimal partitioning algorithm using the test collection, execute the command:

    $ ./create_freq_index opt ../test/test_data/test_collection test_collection.index.opt --check

where `test/test_data/test_collection` is the _basename_ of the collection, that
is the name without the `.{docs,freqs,sizes}` extensions, and
`test_collection.index.opt` is the filename of the output index. `--check`
perform a verification step to check the correctness of the index.

To perform BM25 queries it is necessary to build an additional file containing
the parameters needed to compute the score, such as the document lengths. The
file can be built with the following command:

    $ ./create_wand_data ../test/test_data/test_collection test_collection.wand

Now it is possible to query the index. The command `queries` parses each line of
the standard input as a tab-separated collection of term-ids, where the i-th
term is the i-th list in the input collection. An example set of queries is
again in `test/test_data`.

    $ ./queries opt and test_collection.index.opt test_collection.wand < ../test/test_data/queries

This performs conjunctive queries (`and`). In place of `and` other operators can
be used (`or`, `wand`, ..., see `queries.cpp`), and also multiple operators
separated by colon (`and:or:wand`).


Example: Optimal Space-Time Tradeoffs
-------------------------------------

To construct an index time-optimal for a given query distribution and space
budget it is necessary to follow the following steps.

First, a block-based index must be built on the collection. This will be used to
collect the block-access statistics.

    $ ./create_freq_index block_optpfor ../test/test_data/test_collection test_collection.index.block_optpfor
    $ ./create_wand_data ../test/test_data/test_collection test_collection.wand

Then, given a sequence of queries sampled from the query distribution, we can
produce the statistics.

    $ ./profile_queries block_optpfor ranked_and:wand:maxscore \
        test_collection.index.block_optpfor test_collection.wand \
        < ../test/test_data/queries \
        > query_profile

To predict the block decoding time we need to measure it on a sample.

    $ ./profile_decoding block_optpfor test_collection.index.block_optpfor 0.1 > decoding_times.json

0.1 is the fraction of sampled blocks. For large indexes a very small number can
be used. The measured times can be used to train the linear model.

    $ ./dec_time_regression.py parse_data decoding_times.json decoding_times.pandas
    $ ./dec_time_regression.py train decoding_times.pandas > linear_weights.tsv

The above script requires Numpy, Scipy, Pandas, and Theano.

We can finally build the new index, for example something slightly smaller than
the `block_optpfor` index generated above.

    $ ./optimal_hybrid_index block_optpfor linear_weights.tsv \
        query_profile test_collection.index.block_optpfor \
        lambdas.bin 4000000 test_collection.index.block_mixed

The critical points computed in the greedy algorithm are cached in the
`lambdas.bin` file, which can be re-used to produce other indexes with different
space-time tradeoffs. To recompute them (for example if the query profile
changes) just delete the file.

We can now query the index.

    $ ./queries block_mixed ranked_and test_collection.index.block_mixed \
        test_collection.wand < ../test/test_data/queries
    ...
    Mean: 9.955
    ...

    $ ./queries block_optpfor ranked_and test_collection.index.block_optpfor \
        test_collection.wand < ../test/test_data/queries
    ...
    Mean: 11.125
    ...

Note that the new index is both faster and smaller than the old one. Of course
we are cheating here because we are testing it with the same queries we trained
it on; on a real application training and test set would be independent.

It is also possible to output a sample of the trade-off curve with the following
command.

    $ ./optimal_hybrid_index block_optpfor linear_weights.tsv \
        query_profile test_collection.index.block_optpfor \
        lambdas.bin 0 lambdas.tsv

The file `lambdas.tsv` will contain a sample of triples (lambda, space,
estimated time).


Collection input format
-----------------------

A _binary sequence_ is a sequence of integers prefixed by its length, where both
the sequence integers and the length are written as 32-bit little-endian
unsigned integers.

A _collection_ consists of 3 files, `<basename>.docs`, `<basename>.freqs`,
`<basename>.sizes`.

* `<basename>.docs` starts with a singleton binary sequence where its only
  integer is the number of documents in the collection. It is then followed by
  one binary sequence for each posting list, in order of term-ids. Each posting
  list contains the sequence of document-ids containing the term.

* `basename.freqs` is composed of a one binary sequence per posting list, where
  each sequence contains the occurrence counts of the postings, aligned with the
  previous file (note however that this file does not have an additional
  singleton list at its beginning).

* `basename.sizes` is composed of a single binary sequence whose length is the
  same as the number of documents in the collection, and the i-th element of the
  sequence is the size (number of terms) of the i-th document.


Authors
-------

* Giuseppe Ottaviano <giuott@gmail.com>
* Rossano Venturini <rossano@di.unipi.it>
* Nicola Tonellotto <nicola.tonellotto@isti.cnr.it>
