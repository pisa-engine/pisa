# Requirements

## Compilers

To compile PISA, you will need a compiler supporting at least the C++17
standard. Our continuous integration pipeline compiles PISA and runs
tests in the following configurations:
- Linux:
    - GCC, versions: 10, 11, 12, 13
    - Clang 15, 16
- MaxOS:
    - XCode 13.2

_Supporting Windows is planned but is currently not being actively
worked on_, mostly due to a combination of man-hour shortage,
prioritization, and no core contributors working on Windows at the
moment. If you want to help us set up a Github workflow for Windows and
work out some issues with compilation, let us know on our [Slack
channel](https://join.slack.com/t/pisa-engine/shared_invite/zt-dbxrm1mf-RtQMZTqxxlhOJsv3GHUErw).

## System Dependencies

Most build dependencies are managed automatically with CMake and git submodules.
However, several dependencies still need to be manually provided:

- `CMake >= 3.0`
- `autoconf`,  `automake`, `libtool`, and `m4` (for building `gumbo-parser`)
- OpenMP (optional)

You can opt in to use some system dependencies instead of those in git
submodules:
* [Google Benchmark](https://github.com/google/benchmark)
  (`PISA_SYSTEM_GOOGLE_BENCHMARK`): this is a dependency used only for
  compiling and running microbenchmarks.
* [oneTBB](https://github.com/oneapi-src/oneTBB) (`PISA_SYSTEM_ONETBB`):
  both build-time and runtime dependency.
* [Boost](https://www.boost.org/) (`PISA_SYSTEM_BOOST`): both build-time
  and runtime dependency.
* [CLI11](https://github.com/CLIUtils/CLI11) (`PISA_SYSTEM_CLI11`):
  build-time only dependency used in command line tools.

For example, to use all the system installation of Boost in your build:

```
cmake -DPISA_SYSTEM_BOOST=ON <source-dir>
```
