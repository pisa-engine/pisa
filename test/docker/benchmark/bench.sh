#!/usr/bin/env bash

set -e

./bin/queries \
    -e block_simdbp \
    -a block_max_wand \
    -i "$WORKDIR/inv.block_simdbp" \
    -w "$WORKDIR/inv.bm25.bmw" \
    -F lowercase -F porter2 \
    --terms "$WORKDIR/fwd.termlex" \
    -k 1000 \
    --scorer bm25 \
    -q "$WORKDIR/topics.robust2004.title"
