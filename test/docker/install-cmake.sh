#!/usr/bin/env bash
wget -q 'https://github.com/Kitware/CMake/releases/download/v3.25.2/cmake-3.25.2-linux-x86_64.tar.gz'
sha256sum -c '/pisa/test/docker/cmake-3.25.2-SHA-256.txt' --ignore-missing
tar -xzvf 'cmake-3.25.2-linux-x86_64.tar.gz'
cp cmake-3.25.2-linux-x86_64/bin/* '/usr/local/bin'
cp -r 'cmake-3.25.2-linux-x86_64/share/cmake-3.25' '/usr/local/share'
