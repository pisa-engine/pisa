<p align="center"><img src="https://pisa-engine.github.io/images/logo250.png" width="250px"></p>

# PISA: Performant Indexes and Search for Academia 

[![Build Status](https://travis-ci.com/pisa-engine/pisa.svg?branch=master)](https://travis-ci.com/pisa-engine/pisa)
[![codecov](https://codecov.io/gh/pisa-engine/pisa/branch/master/graph/badge.svg)](https://codecov.io/gh/pisa-engine/pisa)
[![Documentation Status](https://readthedocs.org/projects/pisa/badge/?version=latest)](https://pisa.readthedocs.io/en/latest/?badge=latest)
[![GitHub issues](https://img.shields.io/github/issues/pisa-engine/pisa.svg)](https://github.com/pisa-engine/pisa/issues)
[![GitHub forks](https://img.shields.io/github/forks/pisa-engine/pisa.svg)](https://github.com/pisa-engine/pisa/network)
[![GitHub stars](https://img.shields.io/github/stars/pisa-engine/pisa.svg)](https://github.com/pisa-engine/pisa/stargazers)
[![PRs Welcome](https://img.shields.io/badge/PRs-welcome-brightgreen.svg)](https://github.com/pisa-engine/pisa/pulls)


[Official Documentation](http://pisa.readthedocs.io)

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
    
## Run unit tests

To run the unit tests simply perform a `make test`.

The directory `test/test_data` contains a small document collection used in the
unit tests. The binary format of the collection is described in a following
section.
An example set of queries can also be found in `test/test_data/queries`.

#### Credits
Pisa is an active fork of the [ds2i](https://github.com/ot/ds2i/) project started by [Giuseppe Ottaviano](https://github.com/ot) which is currently unmainteined. This project extends and changes following sometimes different design directions.
