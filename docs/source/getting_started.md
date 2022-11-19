# Getting Started

## Building the code

### Requirements

Currently, our continuous integration pipeline compiles PISA and runs tests
in the following configurations:
- Linux:
    - GCC 8
    - GCC 9
    - GCC 10
    - GCC 11
    - GCC 11
    - Clang 11
- MaxOS:
    - XCode 13.2

Supporting Windows is planned but is currently not being actively worked on,
mostly due to a combination of man-hour shortage, prioritization, and no
core contributors working on Windows at the moment.
If you want to help us set up a Github workflow for Windows and work out
some issues with compilation, let us know on our
[Slack channel](https://join.slack.com/t/pisa-engine/shared_invite/zt-dbxrm1mf-RtQMZTqxxlhOJsv3GHUErw).

### Dependencies

The runtime dependencies are managed automatically with CMake and git submodules.
However, there are several build dependencies needed:

- `CMake >= 3.0`
- `autoconf`,  `automake`, `libtool`, and `m4` (for building `gumbo-parser`)
- OpenMP (optional)

### Building

The following steps explain how to build PISA.
First, you need the code checked out from Github.
(Alternatively, you can download the tarball and unpack it on your local machine.)

```shell
$ git clone https://github.com/pisa-engine/pisa.git
$ cd pisa
```

Then create a build environment.

```shell
$ mkdir build
$ cd build
```

Finally, configure with CMake and compile:

```shell
$ cmake .. -DCMAKE_BUILD_TYPE=Release
$ make
```

#### Build Types

There are two build types available:
- `Release` (default)
- `Debug`

Use `Debug` only for development, testing, and debugging. It is much slower at runtime.

#### Build Systems

CMake supports configuring for different build systems.
On Linux and Mac, the default is Makefiles, thus, the following two commands are equivalent:

```shell
$ cmake -G ..
$ cmake -G "Unix Makefiles" ..
```

Alternatively to Makefiles, you can configure the project to use Ninja instead:

```shell
$ cmake -G Ninja ..
$ ninja # instead of make
```

Other build systems should work in theory but are not tested.

## Testing

You can run the unit and integration tests with:

```shell
$ ctest
```

The directory `test/test_data` contains a small document collection used in the
unit tests. The binary format of the collection is described in a following
section.
An example set of queries can also be found in `test/test_data/queries`.

## PISA Regression Experiments

+ [Regressions for Disks 4 & 5 (Robust04)](experiments/regression-robust04.html)
