PISA_BIN="/home/michal/pisa/build/bin"
INTERSECT_BIN="/home/michal/intersect/target/release/intersect"
BINARY_FREQ_COLL="/home/amallia/cw09b/CW09B.inv"
FWD="/home/amallia/cw09b/CW09B.fwd"
ENCODING="pef" # v1
BASENAME="/data/michal/work/v1/cw09b/cw09b-pef"
THREADS=4
QUERIES="/home/michal/biscorer/data/queries/05.efficiency_topics.no_dups.1k"
#QUERIES="/home/michal/topics.web.51-200.jl"
K=1000
OUTPUT_DIR="/data/michal/intersect/cw09b-est-pef"
#OUTPUT_DIR="/data/michal/intersect/cw09b-est-top20"
FILTERED_QUERIES="${OUTPUT_DIR}/$(basename ${QUERIES}).filtered"
#THRESHOLDS="/home/michal/biscorer/data/thresholds/cw09b/thresholds.cw09b.top1000.bm25.web.51-200"
#THRESHOLDS="/home/michal/biscorer/data/thresholds/cw09b/thresholds.cw09b.top1000.bm25.05.efficiency_topics.no_dups.1k"
THRESHOLDS="/home/michal/biscorer/data/thresholds/cw09b/thresholds.cw09b.0_01.top15.bm25.05.efficiency_topics.no_dups.1k"
#THRESHOLDS="/home/michal/biscorer/data/thresholds/cw09b/thresholds.cw09b.0_01.top20.bm25.05.efficiency_topics.no_dups.1k"

set -e
set -x

## Compress an inverted index in `binary_freq_collection` format.
${PISA_BIN}/compress -i ${BINARY_FREQ_COLL} --fwd ${FWD} -o ${BASENAME} -j ${THREADS} -e ${ENCODING}

# This will produce both quantized scores and max scores (both quantized and not).
${PISA_BIN}/score -i "${BASENAME}.yml" -j ${THREADS}

# This will produce both quantized scores and max scores (both quantized and not).
${PISA_BIN}/bmscore -i "${BASENAME}.yml" -j ${THREADS} --block-size 128

# Filter out queries witout existing terms.
paste -d: ${QUERIES} ${THRESHOLDS} \
    | jq '{"id": split(":")[0], "query": split(":")[1], "threshold": split(":")[2] | tonumber}' -R -c \
    | ${PISA_BIN}/filter-queries -i ${BASENAME}.yml > ${FILTERED_QUERIES}

# This will produce both quantized scores and max scores (both quantized and not).
${PISA_BIN}/bigram-index -i "${BASENAME}.yml" -q ${FILTERED_QUERIES}

# Extract intersections
${PISA_BIN}/intersection -i "${BASENAME}.yml" -q ${FILTERED_QUERIES} --combinations --mtc 2 \
    | grep -v "\[warning\]" \
    > ${OUTPUT_DIR}/intersections.jl

# Select unigrams
${INTERSECT_BIN} -m graph-greedy ${OUTPUT_DIR}/intersections.jl --max 1 > ${OUTPUT_DIR}/selections.1
${INTERSECT_BIN} -m graph-greedy ${OUTPUT_DIR}/intersections.jl --max 2 > ${OUTPUT_DIR}/selections.2
${INTERSECT_BIN} -m graph-greedy ${OUTPUT_DIR}/intersections.jl \
    --max 2 --scale 1.5 > ${OUTPUT_DIR}/selections.2.scaled-1.5

# Run benchmarks
${PISA_BIN}/query -i "${BASENAME}.yml" -q <(jq 'del(.threshold)' ${FILTERED_QUERIES} -c) --benchmark --algorithm wand > ${OUTPUT_DIR}/bench.wand
${PISA_BIN}/query -i "${BASENAME}.yml" -q <(jq 'del(.threshold)' ${FILTERED_QUERIES} -c) --benchmark --algorithm bmw > ${OUTPUT_DIR}/bench.bmw
${PISA_BIN}/query -i "${BASENAME}.yml" -q <(jq 'del(.threshold)' ${FILTERED_QUERIES} -c) --benchmark --algorithm maxscore > ${OUTPUT_DIR}/bench.maxscore
${PISA_BIN}/query -i "${BASENAME}.yml" -q ${FILTERED_QUERIES} --benchmark --algorithm bmw \
    > ${OUTPUT_DIR}/bench.bmw-threshold
${PISA_BIN}/query -i "${BASENAME}.yml" -q ${FILTERED_QUERIES} --benchmark --algorithm maxscore \
    > ${OUTPUT_DIR}/bench.maxscore-threshold
${PISA_BIN}/query -i "${BASENAME}.yml" -q ${FILTERED_QUERIES} --benchmark --algorithm maxscore-union-lookup \
    > ${OUTPUT_DIR}/bench.maxscore-union-lookup
${PISA_BIN}/query -i "${BASENAME}.yml" -q ${OUTPUT_DIR}/selections.1 --benchmark --algorithm unigram-union-lookup \
    > ${OUTPUT_DIR}/bench.unigram-union-lookup
${PISA_BIN}/query -i "${BASENAME}.yml" -q ${OUTPUT_DIR}/selections.2 --benchmark --algorithm union-lookup \
    > ${OUTPUT_DIR}/bench.union-lookup
${PISA_BIN}/query -i "${BASENAME}.yml" -q ${OUTPUT_DIR}/selections.2 --benchmark --algorithm lookup-union \
    > ${OUTPUT_DIR}/bench.lookup-union
${PISA_BIN}/query -i "${BASENAME}.yml" -q ${OUTPUT_DIR}/selections.2.scaled-1.5 \
    --benchmark --algorithm lookup-union \
    > ${OUTPUT_DIR}/bench.lookup-union.scaled-1.5

# Analyze
${PISA_BIN}/query -i "${BASENAME}.yml" -q <(jq 'del(.threshold)' ${FILTERED_QUERIES} -c) --inspect --algorithm maxscore > ${OUTPUT_DIR}/stats.maxscore
${PISA_BIN}/query -i "${BASENAME}.yml" -q ${FILTERED_QUERIES} --inspect --algorithm maxscore \
    > ${OUTPUT_DIR}/stats.maxscore-threshold
${PISA_BIN}/query -i "${BASENAME}.yml" -q ${FILTERED_QUERIES} --inspect --algorithm maxscore-union-lookup \
    > ${OUTPUT_DIR}/stats.maxscore-union-lookup
${PISA_BIN}/query -i "${BASENAME}.yml" -q ${OUTPUT_DIR}/selections.1 --inspect --algorithm unigram-union-lookup \
    > ${OUTPUT_DIR}/stats.unigram-union-lookup
${PISA_BIN}/query -i "${BASENAME}.yml" -q ${OUTPUT_DIR}/selections.2 --inspect --algorithm union-lookup \
    > ${OUTPUT_DIR}/stats.union-lookup
${PISA_BIN}/query -i "${BASENAME}.yml" -q ${OUTPUT_DIR}/selections.2 --inspect --algorithm lookup-union \
    > ${OUTPUT_DIR}/stats.lookup-union
${PISA_BIN}/query -i "${BASENAME}.yml" -q ${OUTPUT_DIR}/selections.2.scaled-1.5 \
    --inspect --algorithm lookup-union \
    > ${OUTPUT_DIR}/stats.lookup-union.scaled-1.5

# Evaluate
${PISA_BIN}/query -i "${BASENAME}.yml" -q <(jq 'del(.threshold)' ${FILTERED_QUERIES} -c) --algorithm maxscore > "${OUTPUT_DIR}/eval.maxscore"
${PISA_BIN}/query -i "${BASENAME}.yml" -q ${FILTERED_QUERIES} --algorithm maxscore \
    > "${OUTPUT_DIR}/eval.maxscore-threshold"
${PISA_BIN}/query -i "${BASENAME}.yml" -q ${FILTERED_QUERIES} --algorithm maxscore-union-lookup \
    > "${OUTPUT_DIR}/eval.maxscore-union-lookup"
${PISA_BIN}/query -i "${BASENAME}.yml" -q ${OUTPUT_DIR}/selections.1 --algorithm unigram-union-lookup \
    > "${OUTPUT_DIR}/eval.unigram-union-lookup"
${PISA_BIN}/query -i "${BASENAME}.yml" -q ${OUTPUT_DIR}/selections.2 --algorithm union-lookup \
    > "${OUTPUT_DIR}/eval.union-lookup"
${PISA_BIN}/query -i "${BASENAME}.yml" -q ${OUTPUT_DIR}/selections.2 --algorithm lookup-union \
    > "${OUTPUT_DIR}/eval.lookup-union"
${PISA_BIN}/query -i "${BASENAME}.yml" -q ${OUTPUT_DIR}/selections.2.scaled-1.5 --algorithm lookup-union \
    > "${OUTPUT_DIR}/eval.lookup-union.scaled-1.5"
