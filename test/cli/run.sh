#!/usr/bin/env bash

DIR=$(dirname "$0")
bats $DIR/setup.sh
bats $DIR/test_taily_stats.sh
bats $DIR/test_count_postings.sh
