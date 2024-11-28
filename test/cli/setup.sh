#!/usr/bin/env bash

# This script should be executed within the build directory that is directly
# in the project directory, e.g., /path/to/pisa/build

command -v compress_inverted_index >/dev/null 2>&1 || {
    echo >&2 "tools not available in default path"
    exit 1
}

test_dir=${TEST_DIR:-../test}

cat "$test_dir/test_data/clueweb1k.plaintext" | parse_collection \
    -F lowercase porter2 \
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
    --shard-files "$test_dir"/test_data/clueweb1k.shard.*

shards invert \
    --input "./fwd.shard" \
    --output "./inv.shard"
