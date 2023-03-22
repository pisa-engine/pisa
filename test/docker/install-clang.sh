#!/usr/bin/env bash

set -e

[[ -z "$1" ]] && {
    echo 'usage: install-clang.sh <clang-version>' 1>&2
    exit 1
}

version="$1"
shift
sources_file='/etc/apt/sources.list.d/llvm.list'

echo 'deb http://apt.llvm.org/bullseye/ llvm-toolchain-bullseye main' > "$sources_file"
echo 'deb-src http://apt.llvm.org/bullseye/ llvm-toolchain-bullseye main' >> "$sources_file"
echo "deb http://apt.llvm.org/bullseye/ llvm-toolchain-bullseye-$version main" >> "$sources_file"
echo "deb-src http://apt.llvm.org/bullseye/ llvm-toolchain-bullseye-$version main" >> "$sources_file"

apt-get update
apt-get -y install wget sudo gnupg cmake libtool git
wget -O - https://apt.llvm.org/llvm-snapshot.gpg.key | sudo apt-key add -
apt-get update
apt-get -y install \
    "clang-$version" \
    "lldb-$version" \
    "lld-$version" \
    "libc++-$version-dev" \
    "libc++abi-$version-dev" \
    "libunwind-$version-dev" "$@"
apt-get clean
rm -rf /var/lib/apt/lists/*

# find "/usr/lib/llvm-$version/" -type f
for f in /usr/lib/llvm-$version/bin/*
do
    ln -sf "$f" /usr/bin
done
