#!/usr/bin/env bats

set +x

PISA_BIN="bin"
export PATH="$PISA_BIN:$PATH"
DIR=$(dirname "$0")

@test "Extract posting counts" {
    expected_1=$(read_collection -c inv.docs entry 1 | wc -w)
    expected_2=$(read_collection -c inv.docs entry 2 | wc -w)
    actual_1=$(echo 0 | count-postings -e block_simdbp -i simdbp)
    actual_2=$(echo 1 | count-postings -e block_simdbp -i simdbp)
    expected_sum=$((expected_1 + expected_2))
    actual_sum=$(echo 0 1 | count-postings -e block_simdbp -i simdbp --sum)
    [[ "$expected_1" = "$actual_1" ]]
    [[ "$expected_2" = "$actual_2" ]]
    [[ "$expected_sum" = "$actual_sum" ]]
}
