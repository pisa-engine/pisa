#!/usr/bin/env bats

PISA_BIN="bin"
export PATH="$PISA_BIN:$PATH"

function write_lines {
    file=$1
    rm -f "$file"
    shift
    for line in "$@"
    do
        echo "$line" >> "$file"
    done
}


function setup {
    write_lines "$BATS_TMPDIR/queries.txt" "brooklyn tea house" "labradoodle" 'Tell your dog I said "hi"'
    write_lines "$BATS_TMPDIR/stopwords.txt" "i" "your"
}

@test "Filter from plain" {
    result=$(cat $BATS_TMPDIR/queries.txt | filter-queries --stemmer porter2 --terms ./fwd.termlex)
    echo "$result" > "$BATS_TMPDIR/result"
    expected='{"query":"brooklyn tea house","term_ids":[6535,29194,15462],"terms":["brooklyn","tea","hous"]}
{"query":"Tell your dog I said \"hi\"","term_ids":[29287,32766,10396,15670,26032,15114],"terms":["tell","your","dog","i","said","hi"]}'
    [[ "$result" = "$expected" ]]
}

@test "Filter with minimum length" {
    result=$(cat $BATS_TMPDIR/queries.txt | filter-queries --min 4 --stemmer porter2 --terms ./fwd.termlex)
    echo "$result" > "$BATS_TMPDIR/result"
    expected='{"query":"Tell your dog I said \"hi\"","term_ids":[29287,32766,10396,15670,26032,15114],"terms":["tell","your","dog","i","said","hi"]}'
    [[ "$result" = "$expected" ]]
}

@test "Filter with maximum length" {
    result=$(cat $BATS_TMPDIR/queries.txt | filter-queries --max 4 --stemmer porter2 --terms ./fwd.termlex)
    echo "$result" > "$BATS_TMPDIR/result"
    expected='{"query":"brooklyn tea house","term_ids":[6535,29194,15462],"terms":["brooklyn","tea","hous"]}'
    [[ "$result" = "$expected" ]]
}

@test "Filter with stopwords" {
    result=$(cat $BATS_TMPDIR/queries.txt | filter-queries --stopwords "$BATS_TMPDIR/stopwords.txt" --stemmer porter2 --terms ./fwd.termlex)
    echo "$result" > "$BATS_TMPDIR/result"
    expected='{"query":"brooklyn tea house","term_ids":[6535,29194,15462],"terms":["brooklyn","tea","hous"]}
{"query":"Tell your dog I said \"hi\"","term_ids":[29287,10396,26032,15114],"terms":["tell","dog","said","hi"]}'
    [[ "$result" = "$expected" ]]
}

@test "Filter without stemmer" {
    result=$(cat $BATS_TMPDIR/queries.txt | filter-queries --terms ./fwd.termlex)
    echo "$result" > "$BATS_TMPDIR/result"
    expected='{"query":"brooklyn tea house","term_ids":[6535,29194],"terms":["brooklyn","tea"]}
{"query":"Tell your dog I said \"hi\"","term_ids":[29287,32766,10396,15670,26032,15114],"terms":["tell","your","dog","i","said","hi"]}'
    [[ "$result" = "$expected" ]]
}
