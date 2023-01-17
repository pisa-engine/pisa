#!/usr/bin/env bash

ctest -j 4
lcov --capture --directory . --output-file coverage.info
lcov --remove coverage.info '/usr/*' --output-file coverage.info # filter system-files
lcov --remove coverage.info '**/external/*' --output-file coverage.info # filter external folder
lcov --remove coverage.info '**/test/*' --output-file coverage.info # filter tests
lcov --list coverage.info # debug info

curl -Os https://uploader.codecov.io/latest/linux/codecov

chmod +x codecov
./codecov -t "$CODECOV_TOKEN"
