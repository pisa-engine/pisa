#!/usr/bin/env bash

DIR=$(dirname "$0")
bash "$DIR/setup.sh"
bats "$DIR/test_taily_stats.sh"
bats "$DIR/test_count_postings.sh"
bats "$DIR/test_wand_data.sh"
bats "$DIR/test_lookup_table.sh"
