Getting Started
===============

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
