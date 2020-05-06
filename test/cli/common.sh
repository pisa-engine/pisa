#!/usr/bin/env bats

function write_lines {
    file=$1
    rm -f "$file"
    shift
    for line in "$@"
    do
        echo "$line" >> "$file"
    done
}
