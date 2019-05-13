#!/bin/sh

PISA_PREFIX=~/src/pisa
PISA_BIN=$PISA_PREFIX/build/bin
THREADS=20

COLLECTION=(/research/remote/collections/TREC/disk4/fr.gz
	    /research/remote/collections/TREC/disk4/ft.gz
	    /research/remote/collections/TREC/disk5/fbis.gz
	    /research/remote/collections/TREC/disk5/latimes.gz)

ROB_PREFIX=./robust04

# Generate robust04 robust04.terms robust04.urls robust04.documents
zcat ${COLLECTION[@]} | $PISA_BIN/parse_collection -j $THREADS -f trectext --content-parser html --stemmer krovetz -o $ROB_PREFIX

# Generate robust04.docs robust04.freqs robust04.sizes
$PISA_BIN/invert -i $ROB_PREFIX -o $ROB_PREFIX -j 40 --term-count $(wc -l < $ROB_PREFIX.terms)

# Generate robust04.index.opt
$PISA_BIN/create_freq_index -t opt -c $ROB_PREFIX -o $ROB_PREFIX.index.opt --check

# Generate robust04.wand
$PISA_BIN/create_wand_data -c $ROB_PREFIX -o $ROB_PREFIX.wand

# Test queries
$PISA_BIN/queries -t opt -a block_max_wand -i $ROB_PREFIX.index.opt -w $ROB_PREFIX.wand -q $PISA_PREFIX/test/test_data/queries

# Test indri output
echo '301:international organized crime' | $PISA_BIN/evaluate_queries -t opt -a block_max_wand -i $ROB_PREFIX.index.opt -w $ROB_PREFIX.wand --documents $ROB_PREFIX.doclex --terms $ROB_PREFIX.termlex
