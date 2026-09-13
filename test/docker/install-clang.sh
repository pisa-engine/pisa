#!/usr/bin/env bash

set -e

[[ -z "$1" ]] && {
    echo 'usage: install-clang.sh <clang-version>' 1>&2
    exit 1
}

version="$1"
shift

apt-get update
apt-get -y install lsb-release software-properties-common wget curl sudo gnupg cmake libtool git ca-certificates
/llvm.sh "$version" all

apt-get clean
rm -rf /var/lib/apt/lists/*

# find "/usr/lib/llvm-$version/" -type f
for f in /usr/lib/llvm-${version}/bin/*
do
    ln -sf "$f" /usr/bin
done
