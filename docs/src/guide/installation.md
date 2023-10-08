# Installation

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
$ cmake ..
$ make
```

## Build Types

There are two build types available:
- `Release` (default)
- `Debug`
- `RelWithDebInfo`
- `MinSizeRel`

Use `Debug` only for development, testing, and debugging. It is much slower at runtime.

Learn more from [CMake documentation](https://cmake.org/cmake/help/latest/variable/CMAKE_BUILD_TYPE.html).

## Build Systems

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
