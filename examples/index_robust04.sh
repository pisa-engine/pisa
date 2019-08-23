#!/bin/sh

set -e

THREADS=$(nproc)

COLLECTION=(/research/remote/collections/TREC/disk4/fr.gz
	    /research/remote/collections/TREC/disk4/ft.gz
	    /research/remote/collections/TREC/disk5/fbis.gz
	    /research/remote/collections/TREC/disk5/latimes.gz)

ROB_PREFIX=./robust04

# Generate robust04 robust04.terms robust04.urls robust04.documents
zcat ${COLLECTION[@]} | parse_collection -j $THREADS -f trectext --content-parser html --stemmer krovetz -o $ROB_PREFIX

# Generate robust04.docs robust04.freqs robust04.sizes
invert -i $ROB_PREFIX -o $ROB_PREFIX -j $THREADS --term-count $(wc -l < $ROB_PREFIX.terms)

# Generate robust04.index.opt
create_freq_index -t pefopt -c $ROB_PREFIX -o $ROB_PREFIX.index.opt --check

# Generate robust04.wand
create_wand_data -c $ROB_PREFIX -o $ROB_PREFIX.wand

# Test TREC output
echo '301:international organized crime' | evaluate_queries -t pefopt -a block_max_wand -i $ROB_PREFIX.index.opt -w $ROB_PREFIX.wand --documents $ROB_PREFIX.doclex --terms $ROB_PREFIX.termlex
