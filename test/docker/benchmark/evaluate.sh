#!/usr/bin/env bash

set -e

source ./encodings.sh

for encoding in ${ENCODINGS[@]}; do
    ./bin/evaluate_queries \
        -e "$encoding" \
        -a block_max_wand \
        -i "$WORKDIR/inv.$encoding" \
        -w "$WORKDIR/inv.bm25.bmw" \
        -F lowercase -F porter2 \
        --terms "$WORKDIR/fwd.termlex" \
        --documents "$WORKDIR/fwd.bp.doclex" \
        -k 1000 \
        --scorer bm25 \
        -q "$WORKDIR/topics.robust2004.title" \
        > "$WORKDIR/results.txt"

    trec_eval -m map -m P.30 -m ndcg_cut.20 "$WORKDIR/qrels.robust2004.txt" "$WORKDIR/results.txt" > 'eval.txt'
    cat 'eval.txt'
    diff 'eval.txt' expected-eval.txt
done
