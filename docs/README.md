# Generating Documentation

## Requirements

* `make`
* `mdbook`
* `mdbook-cmdrun`
* PISA build with CLI tools available at `../build/bin`

Install mdBook and cmdrun plugin with:

```
cargo install mdbook
cargo install mdbook-cmdrun
```

Build PISA with `PISA_BUILD_TOOLS=ON` (the default):

```
PISA_SRC_DIR=`git rev-parse --show-toplevel`
PISA_BUILD_DIR="$PISA_SRC_DIR/build"
cmake \
    -DCMAKE_BUILD_TYPE=Release \
    -G Ninja \
    -S "$PISA_SRC_DIR" \
    -B "$PISA_SRC_DIR/build"
cmake --build "$PISA_BUILD_DIR" --verbose -j
```

## Generate

Generate by calling the following _in this directory_:

```
make
```
