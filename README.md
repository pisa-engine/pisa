<p align="center"><img src="https://pisa-engine.github.io/images/logo250.png" width="250px"></p>

# Pisa: Performant Indexes and Search for Academia 

[![Build Status](https://travis-ci.com/pisa-engine/pisa.svg?branch=master)](https://travis-ci.com/pisa-engine/pisa)
[![codecov](https://codecov.io/gh/pisa-engine/pisa/branch/master/graph/badge.svg)](https://codecov.io/gh/pisa-engine/pisa)

## Building the code

The code is tested on Linux with `GCC 7.4.0`, `GCC 8.1.0`, `Clang 5.0`, `Clang 6.0` and on macOS with `AppleClang 9.1.0`.

The following dependencies are needed for the build.

* CMake >= 3.0, for the build system
* OpenMP (optional)

To build the code:

    $ mkdir build
    $ cd build
    $ cmake .. -DCMAKE_BUILD_TYPE=Release
    $ make

## Usage

### Create an index

To create an index use the command `create_freq_index`. The available index
types are listed in `index_types.hpp`. 

    create_freq_index - a tool for creating an index.
    Usage:
      create_freq_index [OPTION...]

      -h, --help                 Print help
      -t, --type type_name       Index type
      -c, --collection basename  Collection basename
      -o, --out filename         Output filename
          --check                Check the correctness of the index (default:
                                 false) 

For example, to create an index using the
optimal partitioning algorithm using the test collection, execute the command:

    $ ./bin/create_freq_index -t opt -c ../test/test_data/test_collection -o test_collection.index.opt --check

where `test/test_data/test_collection` is the _basename_ of the collection, that
is the name without the `.{docs,freqs,sizes}` extensions, and
`test_collection.index.opt` is the filename of the output index. `--check`
perform a verification step to check the correctness of the index.

### Build additional data

To perform BM25 queries it is necessary to build an additional file containing
the parameters needed to compute the score, such as the document lengths. The
file can be built with the following command:

    $ ./bin/create_wand_data -c ../test/test_data/test_collection -o test_collection.wand

If you want to compress the file append `--compress` at the end of the command.


### Query an index

Now it is possible to query the index. The command `queries` parses each line of the standard input as a tab-separated collection of term-ids, where the i-th
term is the i-th list in the input collection.

    $ ./bin/queries -t opt -a and -i test_collection.index.opt -w test_collection.wand -q ../test/test_data/queries

This performs conjunctive queries (`and`). In place of `and` other operators can
be used (`or`, `wand`, ..., see `queries.cpp`), and also multiple operators
separated by colon (`and:or:wand`).

If the WAND file is compressed, please append `--compressed-wand` flag.

## Run unit tests

To run the unit tests simply perform a `make test`.

The directory `test/test_data` contains a small document collection used in the
unit tests. The binary format of the collection is described in a following
section.
An example set of queries can also be found in `test/test_data/queries`.

## Collection input format

A _binary sequence_ is a sequence of integers prefixed by its length, where both
the sequence integers and the length are written as 32-bit little-endian
unsigned integers.

A _collection_ consists of 3 files, `<basename>.docs`, `<basename>.freqs`,
`<basename>.sizes`.

* `<basename>.docs` starts with a singleton binary sequence where its only
  integer is the number of documents in the collection. It is then followed by
  one binary sequence for each posting list, in order of term-ids. Each posting
  list contains the sequence of document-ids containing the term.

* `<basename>.freqs` is composed of a one binary sequence per posting list, where
  each sequence contains the occurrence counts of the postings, aligned with the
  previous file (note however that this file does not have an additional
  singleton list at its beginning).

* `<basename>.sizes` is composed of a single binary sequence whose length is the
  same as the number of documents in the collection, and the i-th element of the
  sequence is the size (number of terms) of the i-th document.

#### Credits
Pisa is an active fork of the [ds2i](https://github.com/ot/ds2i/) project started by [Giuseppe Ottaviano](https://github.com/ot) which is currently unmainteined. This project extends and changes following sometimes different design directions.
