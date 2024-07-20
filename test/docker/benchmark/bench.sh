#!/usr/bin/env bash

set -e

source ./encodings.sh

for encoding in ${ENCODINGS[@]}; do
    ./bin/queries \
        -e "$encoding" \
        -a block_max_wand \
        -i "$WORKDIR/inv.$encoding" \
        -w "$WORKDIR/inv.bm25.bmw" \
        -F lowercase -F porter2 \
        --terms "$WORKDIR/fwd.termlex" \
        -k 1000 \
        --scorer bm25 \
        -q "$WORKDIR/topics.robust2004.title"
done
