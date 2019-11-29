PISA_BIN="/home/michal/work/pisa/build/bin"
INTERSECT_BIN="/home/michal/intersect/target/release/intersect"
BINARY_FREQ_COLL="/mnt/michal/work/cw09b/inv"
FWD="/mnt/michal/work/cw09b/fwd"
INV="/mnt/michal/work/cw09b/inv"
BASENAME="/mnt/michal/work/v1/cw09b/cw09b"
THREADS=16
TYPE="block_simdbp" # v0.6
ENCODING="simdbp" # v1
QUERIES="/home/michal/biscorer/data/queries/05.efficiency_topics.no_dups.1k"
K=1000
OUTPUT_DIR=`pwd`
FILTERED_QUERIES="${OUTPUT_DIR}/filtered_queries"

set -x
set -e

## Compress an inverted index in `binary_freq_collection` format.
#./bin/compress -i ${BINARY_FREQ_COLL} --fwd ${FWD} -o ${BASENAME} -j ${THREADS} -e ${ENCODING}
#
## This will produce both quantized scores and max scores (both quantized and not).
#./bin/bigram-index -i "${BASENAME}.yml" -q ${QUERIES}
#
## This will produce both quantized scores and max scores (both quantized and not).
#./bin/score -i "${BASENAME}.yml" -j ${THREADS}

# Filter out queries witout existing terms.
${PISA_BIN}/filter-queries -i ${BASENAME}.yml -q ${QUERIES} | grep -v "\[warning\]" \
    > ${FILTERED_QUERIES}

# Extract thresholds (TODO: estimates)
${PISA_BIN}/thresholds -t ${TYPE} -i ${INV}.${TYPE} \
    -w ${INV}.wand -q ${FILTERED_QUERIES} -k ${K} --terms "${FWD}.termlex" --stemmer porter2 \
   | grep -v "\[warning\]" \
    > ${OUTPUT_DIR}/thresholds
cut -d: -f1 ${FILTERED_QUERIES} | paste - ${OUTPUT_DIR}/thresholds > ${OUTPUT_DIR}/thresholds.tsv

# Extract intersections
${PISA_BIN}/compute_intersection -t ${TYPE} -i ${INV}.${TYPE} \
    -w ${INV}.wand -q ${FILTERED_QUERIES} --combinations --terms "${FWD}.termlex" --stemmer porter2 \
    | grep -v "\[warning\]" \
    > ${OUTPUT_DIR}/intersections.tsv

# Select unigrams
${INTERSECT_BIN} -t ${OUTPUT_DIR}/thresholds.tsv -m graph-greedy ${OUTPUT_DIR}/intersections.tsv \
    --terse --max 1 > ${OUTPUT_DIR}/selections.1

# Select unigrams and bigrams
${INTERSECT_BIN} -t ${OUTPUT_DIR}/thresholds.tsv -m graph-greedy ${OUTPUT_DIR}/intersections.tsv \
    --terse --max 2 > ${OUTPUT_DIR}/selections.2

# Run benchmarks
#${PISA_BIN}/query -i "${BASENAME}.yml" -q ${FILTERED_QUERIES} --benchmark --algorithm maxscore
#${PISA_BIN}/query -i "${BASENAME}.yml" -q ${FILTERED_QUERIES} --benchmark --algorithm maxscore \
#    --thresholds ${OUTPUT_DIR}/thresholds
#${PISA_BIN}/query -i "${BASENAME}.yml" -q ${FILTERED_QUERIES} --benchmark --algorithm maxscore-union-lookup \
#    --thresholds ${OUTPUT_DIR}/thresholds
#${PISA_BIN}/query -i "${BASENAME}.yml" -q ${FILTERED_QUERIES} --benchmark --algorithm unigram-union-lookup \
#    --thresholds ${OUTPUT_DIR}/thresholds --intersections ${OUTPUT_DIR}/selections.1
#${PISA_BIN}/query -i "${BASENAME}.yml" -q ${FILTERED_QUERIES} --benchmark --algorithm union-lookup \
#    --thresholds ${OUTPUT_DIR}/thresholds --intersections ${OUTPUT_DIR}/selections.1
#${PISA_BIN}/query -i "${BASENAME}.yml" -q ${FILTERED_QUERIES} --benchmark --algorithm union-lookup \
#    --thresholds ${OUTPUT_DIR}/thresholds --intersections ${OUTPUT_DIR}/selections.2
${PISA_BIN}/query -i "${BASENAME}.yml" -q ${FILTERED_QUERIES} --benchmark --algorithm two-phase-union-lookup \
    --thresholds ${OUTPUT_DIR}/thresholds --intersections ${OUTPUT_DIR}/selections.2
