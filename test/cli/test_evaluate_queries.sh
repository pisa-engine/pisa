#!/usr/bin/env bats

PISA_BIN="bin"
export PATH="$PISA_BIN:$PATH"

. "$BATS_TEST_DIRNAME/common.sh"

function setup {
    write_lines "$BATS_TMPDIR/queries.txt" "brooklyn tea house" "the" 'Tell your dog I said "hi"'
    write_lines "$BATS_TMPDIR/queries_with_ids.txt" "2:brooklyn tea house" "0:the" '1:Tell your dog I said "hi"'
    jq '{"query": .}' "$BATS_TMPDIR/queries.txt" -Rc > "$BATS_TMPDIR/queries.jl"
    jq '{"id": .|split(":")[0], "query": .|split(":")[1] }' "$BATS_TMPDIR/queries_with_ids.txt" -Rc \
        > "$BATS_TMPDIR/queries_with_ids.jl"
}

@test "From echo - ID is 0" {
    result=$(echo "brooklyn tea house" | ./bin/evaluate_queries -e block_simdbp -i ./simdbp --stemmer porter2 \
                -k 3 -a wand --scorer bm25 --documents ./fwd.doclex --terms ./fwd.termlex -w ./bm25.bmw \
                | cut -f1)
    expected=$(printf "0\n0\n0")
    [[ "$result" = "$expected" ]]
}

@test "From plan text - consecutive IDs" {
    result=$(cat $BATS_TMPDIR/queries.txt | ./bin/evaluate_queries -e block_simdbp -i ./simdbp --stemmer porter2 \
                -k 3 -a wand --scorer bm25 --documents ./fwd.doclex --terms ./fwd.termlex -w ./bm25.bmw \
                | cut -f1)
    expected=$(printf "0\n0\n0\n1\n1\n1\n2\n2\n2")
    [[ "$result" = "$expected" ]]
}

@test "From plan text - predefined IDs" {
    result=$(cat $BATS_TMPDIR/queries_with_ids.txt | ./bin/evaluate_queries -e block_simdbp \
                -i ./simdbp --stemmer porter2 -k 3 -a wand --scorer bm25 \
                --documents ./fwd.doclex --terms ./fwd.termlex -w ./bm25.bmw \
                | cut -f1)
    expected=$(printf "2\n2\n2\n0\n0\n0\n1\n1\n1")
    [[ "$result" = "$expected" ]]
}

@test "From JSON without IDs" {
    result=$(cat $BATS_TMPDIR/queries.jl | ./bin/evaluate_queries -e block_simdbp \
                -i ./simdbp --stemmer porter2 -k 3 -a wand --scorer bm25 \
                --documents ./fwd.doclex --terms ./fwd.termlex -w ./bm25.bmw \
                | cut -f1)
    expected=$(printf "0\n0\n0\n1\n1\n1\n2\n2\n2")
    [[ "$result" = "$expected" ]]
}

@test "From JSON with IDs" {
    result=$(cat $BATS_TMPDIR/queries_with_ids.jl | ./bin/evaluate_queries -e block_simdbp \
                -i ./simdbp --stemmer porter2 -k 3 -a wand --scorer bm25 \
                --documents ./fwd.doclex --terms ./fwd.termlex -w ./bm25.bmw \
                | cut -f1)
    expected=$(printf "2\n2\n2\n0\n0\n0\n1\n1\n1")
    [[ "$result" = "$expected" ]]
}
