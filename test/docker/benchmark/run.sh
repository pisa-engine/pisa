#!/usr/bin/env bash

set -e

./setup.sh
./build.sh
./evaluate.sh
./bench.sh
