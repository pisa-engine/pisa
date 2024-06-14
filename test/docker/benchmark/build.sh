#!/usr/bin/env bash

set -e

source ./encodings.sh

gzip -dc $(find "$COLLECTION_PATH" -type f -name '*.*z' \
    \( -path '*/disk4/fr94/[0-9]*/*' -o -path '*/disk4/ft/ft*' \
    -o -path '*/disk5/fbis/fb*' -o -path '*/disk5/latimes/la*' \)) \
    | ./bin/parse_collection -f trectext -b 10000 -F lowercase -F porter2 --html -o "$WORKDIR/fwd"

./bin/invert \
    -i "$WORKDIR/fwd" \
    -o "$WORKDIR/inv" \
    --batch-size 400000

./bin/reorder-docids \
    --bp \
    -c "$WORKDIR/inv" \
    -o "$WORKDIR/inv.bp" \
    --documents "$WORKDIR/fwd.doclex" \
    --reordered-documents "$WORKDIR/fwd.bp.doclex"

./bin/create_wand_data \
    -c "$WORKDIR/inv.bp" \
    -b 64 \
    -o "$WORKDIR/inv.bm25.bmw" \
    -s bm25

for encoding in ${ENCODINGS[@]}; do
    ./bin/compress_inverted_index \
        -e "$encoding" \
        -c "$WORKDIR/inv.bp" \
        -o "$WORKDIR/inv.$encoding" \
        --check
done
