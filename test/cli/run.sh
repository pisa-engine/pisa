#!/usr/bin/env bash

DIR=$(dirname "$0")
$DIR/setup.sh
bats $DIR/test_filter_queries.sh
bats $DIR/test_compute_intersection.sh
bats $DIR/test_taily_stats.sh
