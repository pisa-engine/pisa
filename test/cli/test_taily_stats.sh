#!/usr/bin/env bats

set +x

PISA_BIN="bin"
export PATH="$PISA_BIN:$PATH"
DIR=$(dirname "$0")

echo_int () {
    if (( $1 >= 0 && $1 <= 255 ))
    then
        printf "\\x$(printf "%x" $1)"
        printf "\\x00"
        printf "\\x00"
        printf "\\x00"
    else
        printf "Invalid value\n" >&2
        return 1
    fi
}

write_documents () {
    # number of docs
    echo_int 1
    echo_int 5

    # term 0
    echo_int 2 # len
    echo_int 0
    echo_int 2

    # term 1
    echo_int 3 # len
    echo_int 1
    echo_int 3
    echo_int 4

    # term 2
    echo_int 1 # len
    echo_int 5
}

write_frequencies () {
    # term 0
    echo_int 2 # len
    echo_int 1
    echo_int 1

    # term 1
    echo_int 3 # len
    echo_int 2
    echo_int 1
    echo_int 5

    # term 2
    echo_int 1 # len
    echo_int 4
}

write_sizes () {
    echo_int 5

    echo_int 1
    echo_int 1
    echo_int 1
    echo_int 1
    echo_int 1
}

@test "Extract Taily features" {
    write_documents > "$BATS_TMPDIR/coll.docs"
    write_frequencies > "$BATS_TMPDIR/coll.freqs"
    write_sizes > "$BATS_TMPDIR/coll.sizes"
    create_wand_data -c "$BATS_TMPDIR/coll" -o "$BATS_TMPDIR/coll.wand" -s quantized -b 16
    taily-stats -c "$BATS_TMPDIR/coll" -o "$BATS_TMPDIR/out" -w "$BATS_TMPDIR/coll.wand" -s quantized
    size=$(ls -l "$BATS_TMPDIR/out" | cut -d" " -f5)
    echo $size
    [[ "$size" = "88" ]]
}

@test "Extract Taily features for shards and score" {
    cat "$BATS_TEST_DIRNAME/../test_data/clueweb1k.plaintext" | parse_collection \
        --stemmer porter2 \
        --output "$BATS_TMPDIR/fwd" \
        --format plaintext

    echo "=== Partition"
    partition_fwd_index \
        --input "$BATS_TMPDIR/fwd" \
        --output "$BATS_TMPDIR/fwd.shard" \
        --shard-files $BATS_TEST_DIRNAME/../test_data/clueweb1k.shard.*

    echo "=== Invert index"
    invert \
        --input "$BATS_TMPDIR/fwd" \
        --output "$BATS_TMPDIR/inv"

    echo "=== Wand data for index"
    create_wand_data \
        --collection "$BATS_TMPDIR/inv" \
        --output "$BATS_TMPDIR/bm25" \
        --scorer bm25 \
        --block-size 32

    echo "=== Invert shards"
    shards invert \
        --input "$BATS_TMPDIR/fwd.shard" \
        --output "$BATS_TMPDIR/inv.shard"

    echo "=== Wand data for shards"
    shards wand-data \
        --collection "$BATS_TMPDIR/inv.shard" \
        --output "$BATS_TMPDIR/bm25.shard" \
        --scorer bm25 \
        --block-size 32

    echo "=== Global Taily stats"
    taily-stats \
        -c "$BATS_TMPDIR/inv" \
        -o "$BATS_TMPDIR/taily-stats" \
        -w "$BATS_TMPDIR/bm25" \
        -s bm25

    echo "=== Shard Taily stats"
    shards taily-stats \
        -c "$BATS_TMPDIR/inv.shard" \
        -o "$BATS_TMPDIR/taily-stats" \
        -w "$BATS_TMPDIR/bm25.shard" \
        -s bm25

    echo "=== Write some queries"
    echo "website" > "$BATS_TMPDIR/queries"
    echo "home video" >> "$BATS_TMPDIR/queries"
    echo "protect privacy" >> "$BATS_TMPDIR/queries"
    echo "gibberisshhhh" >> "$BATS_TMPDIR/queries"
    echo "copyright process" >> "$BATS_TMPDIR/queries"

    echo "=== Shard Taily scores"
    shards taily-score \
        --global-stats "$BATS_TMPDIR/taily-stats" \
        --shard-stats "$BATS_TMPDIR/taily-stats" \
        -k 10 \
        --terms "$BATS_TMPDIR/fwd.termlex" \
        --shard-terms "$BATS_TMPDIR/fwd.shard.{}.termlex" \
        --stemmer porter2 \
        -q "$BATS_TMPDIR/queries" \
        > "$BATS_TMPDIR/scores"

    count=$(ls -l $BATS_TMPDIR/taily-stats* | wc -l)
    [[ "$count" = "5" ]]

    count=$(ls -l $BATS_TMPDIR/taily-stats.00* | wc -l)
    [[ "$count" = "4" ]]

    count=$(cat $BATS_TMPDIR/scores | wc -l)
    [[ "$count" = "5" ]]

    count=$(cat $BATS_TMPDIR/scores | jq '.scores' -c | wc -l)
    [[ "$count" = "5" ]]

    count=$(cat $BATS_TMPDIR/scores | jq '.time' -c | wc -l)
    [[ "$count" = "5" ]]

    count=$(cat $BATS_TMPDIR/scores | jq 'select(.scores==[0.0,0.0,0.0,0.0])' -c | wc -l)
    [[ "$count" = "1" ]]
}
