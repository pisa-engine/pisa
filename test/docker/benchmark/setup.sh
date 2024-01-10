#!/usr/bin/env bash

set -e

wget http://trec.nist.gov/data/robust/04.testset.gz
gunzip 04.testset.gz
./bin/extract_topics -f trec -i 04.testset -o "$WORKDIR/topics.robust2004"

wget http://trec.nist.gov/data/robust/qrels.robust2004.txt
cp qrels.robust2004.txt "$WORKDIR/"

wget https://github.com/usnistgov/trec_eval/archive/refs/tags/v9.0.8.tar.gz
tar -xzvf v9.0.8.tar.gz
cd trec_eval-9.0.8
make install
cd ..
