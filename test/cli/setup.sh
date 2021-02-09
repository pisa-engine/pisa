#!/usr/bin/env bash

# This script should be executed within the build directory that is directly
# in the project directory, e.g., /path/to/pisa/build

PISA_BIN="./bin"
export PATH="$PISA_BIN:$PATH"

test_dir=${TEST_DIR:-../test}
cat "$test_dir/test_data/clueweb1k.plaintext" | parse_collection \
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

partition_fwd_index \
    --input "./fwd" \
    --output "./fwd.shard" \
    --shard-files $TEST_DIR/test_data/clueweb1k.shard.*

shards invert \
    --input "./fwd.shard" \
    --output "./inv.shard"
