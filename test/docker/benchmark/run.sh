#!/bin/bash

set -e

collection_path='/opt/disk45'
workdir='/opt/workdir'

gzip -dc $(find "$collection_path" -type f -name '*.*z' \
    \( -path '*/disk4/fr94/[0-9]*/*' -o -path '*/disk4/ft/ft*' \
    -o -path '*/disk5/fbis/fb*' -o -path '*/disk5/latimes/la*' \)) \
    | ./bin/parse_collection -f trectext -b 10000 -F lowercase -F porter2 --html 1 -o "$workdir/fwd"

./bin/invert \
    -i "$workdir/fwd" \
    -o "$workdir/inv" \
    --batch-size 400000

./bin/reorder-docids \
    --bp \
    -c "$workdir/inv" \
    -o "$workdir/inv.bp" \
    --documents "$workdir/fwd.doclex" \
    --reordered-documents "$workdir/fwd.bp.doclex"

./bin/create_wand_data \
    -c "$workdir/inv.bp" \
    -b 64 \
    -o "$workdir/inv.bm25.bmw" \
    -s bm25

./bin/compress_inverted_index \
    -e block_simdbp \
    -c "$workdir/inv.bp" \
    -o "$workdir/inv.block_simdbp" \
    --check

wget http://trec.nist.gov/data/robust/04.testset.gz
gunzip 04.testset.gz
./bin/extract_topics -f trec -i 04.testset -o "$workdir/topics.robust2004"

./bin/queries \
    -e block_simdbp \
    -a block_max_wand \
    -i "$workdir/inv.block_simdbp" \
    -w "$workdir/inv.bm25.bmw" \
    -F lowercase -F porter2 \
    --terms "$workdir/fwd.termlex" \
    -k 1000 \
    --scorer bm25 \
    -q "$workdir/topics.robust2004.title"
