#!/usr/bin/env bash

# This script should be executed within the build directory that is directly
# in the project directory, e.g., /path/to/pisa/build

PISA_BIN="./bin"
export PATH="$PISA_BIN:$PATH"

cat "../test/test_data/clueweb1k.plaintext" | parse_collection \
    --stemmer porter2 \
    --output "./fwd" \
    --format plaintext

invert --input "./fwd" --output "./inv"

compress_inverted_index --check \
    --encoding block_simdbp \
    --collection "./inv" \
    --output "./simdbp"

create_wand_data \
    --scorer bm25 \
    --collection "./inv" \
    --output "./bm25.bmw" \
    --block-size 32
