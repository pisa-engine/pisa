PISA_BIN="/home/michal/pisa/build/bin"
INTERSECT_BIN="/home/michal/intersect/target/release/intersect"
BINARY_FREQ_COLL="/home/amallia/cw09b/CW09B.inv"
FWD="/home/amallia/cw09b/CW09B.fwd"
INV="/home/amallia/cw09b/CW09B"
BASENAME="/data/michal/work/v1/cw09b/cw09b"
THREADS=4
TYPE="block_simdbp" # v0.6
ENCODING="simdbp" # v1
#QUERIES="/home/michal/biscorer/data/queries/05.efficiency_topics.no_dups.1k"
QUERIES="/home/michal/topics.web.51-200.jl"
K=1000
OUTPUT_DIR="/data/michal/intersect/cw09b"
FILTERED_QUERIES="${OUTPUT_DIR}/topics.web.51-200.filtered"
FILTERED_QUERIES="${OUTPUT_DIR}/topics.web.51-200.filtered"
#THRESHOLDS="/home/michal/biscorer/data/thresholds/cw09b/thresholds.cw09b.top1000.bm25.web.51-200"
THRESHOLDS="${OUTPUT_DIR}/thresholds"

set -e
set -x

## Compress an inverted index in `binary_freq_collection` format.
#./bin/compress -i ${BINARY_FREQ_COLL} --fwd ${FWD} -o ${BASENAME} -j ${THREADS} -e ${ENCODING}

# This will produce both quantized scores and max scores (both quantized and not).
#./bin/score -i "${BASENAME}.yml" -j ${THREADS}

# This will produce both quantized scores and max scores (both quantized and not).
#./bin/bigram-index -i "${BASENAME}.yml" -q ${QUERIES}

# Filter out queries witout existing terms.
#${PISA_BIN}/filter-queries -i ${BASENAME}.yml -q ${QUERIES} | grep -v "\[warning\]" > ${FILTERED_QUERIES}

# Extract thresholds (TODO: estimates)
#${PISA_BIN}/threshold -i ${BASENAME}.yml -q ${FILTERED_QUERIES} -k ${K} --in-place

#####jq '.id + "\t" + (.threshold | tostring)' ${FILTERED_QUERIES} -r > ${OUTPUT_DIR}/thresholds.tsv

# Extract intersections
#${PISA_BIN}/intersection -i "${BASENAME}.yml" -q ${FILTERED_QUERIES} --combinations \
#    | grep -v "\[warning\]" \
#    > ${OUTPUT_DIR}/intersections.jl

# Select unigrams
${INTERSECT_BIN} -m graph-greedy ${OUTPUT_DIR}/intersections.jl --max 1 > ${OUTPUT_DIR}/selections.1
${INTERSECT_BIN} -m graph-greedy ${OUTPUT_DIR}/intersections.jl --max 2 > ${OUTPUT_DIR}/selections.2
${INTERSECT_BIN} -m graph-greedy ${OUTPUT_DIR}/intersections.jl \
    --max 2 --scale 1.5 > ${OUTPUT_DIR}/selections.2.scaled-1.5
${INTERSECT_BIN} -m graph-greedy ${OUTPUT_DIR}/intersections.jl \
    --max 2 --scale 2 > ${OUTPUT_DIR}/selections.2.scaled-2
${INTERSECT_BIN} -m graph-greedy ${OUTPUT_DIR}/intersections.jl \
    --max 2 --scale-by-query-len > ${OUTPUT_DIR}/selections.2.scaled-smart

# Run benchmarks
${PISA_BIN}/query -i "${BASENAME}.yml" -q <(jq 'del(.threshold)' ${FILTERED_QUERIES} -c) --benchmark --algorithm maxscore > ${OUTPUT_DIR}/bench.maxscore
${PISA_BIN}/query -i "${BASENAME}.yml" -q ${FILTERED_QUERIES} --benchmark --algorithm maxscore \
    > ${OUTPUT_DIR}/bench.maxscore-threshold
${PISA_BIN}/query -i "${BASENAME}.yml" -q ${FILTERED_QUERIES} --benchmark --algorithm maxscore-union-lookup \
    > ${OUTPUT_DIR}/bench.maxscore-union-lookup
${PISA_BIN}/query -i "${BASENAME}.yml" -q ${OUTPUT_DIR}/selections.1 --benchmark --algorithm unigram-union-lookup \
    > ${OUTPUT_DIR}/bench.unigram-union-lookup
${PISA_BIN}/query -i "${BASENAME}.yml" -q ${OUTPUT_DIR}/selections.2 --benchmark --algorithm union-lookup \
    > ${OUTPUT_DIR}/bench.union-lookup.2
${PISA_BIN}/query -i "${BASENAME}.yml" -q ${OUTPUT_DIR}/selections.2 --benchmark --algorithm lookup-union \
    > ${OUTPUT_DIR}/bench.lookup-union
#${PISA_BIN}/query -i "${BASENAME}.yml" -q ${FILTERED_QUERIES} --benchmark --algorithm union-lookup \
#    --intersections ${OUTPUT_DIR}/selections.2.scaled-1.5 > ${OUTPUT_DIR}/bench.union-lookup.scaled-1.5
#${PISA_BIN}/query -i "${BASENAME}.yml" -q ${FILTERED_QUERIES} --benchmark --algorithm union-lookup \
#    --intersections ${OUTPUT_DIR}/selections.2.scaled-2 > ${OUTPUT_DIR}/bench.union-lookup.scaled-2
#${PISA_BIN}/query -i "${BASENAME}.yml" -q ${FILTERED_QUERIES} --benchmark --algorithm union-lookup \
#    --intersections ${OUTPUT_DIR}/selections.2.scaled-3 > ${OUTPUT_DIR}/bench.union-lookup.scaled-3
#${PISA_BIN}/query -i "${BASENAME}.yml" -q ${FILTERED_QUERIES} --benchmark --algorithm union-lookup \
#    --intersections ${OUTPUT_DIR}/selections.2.scaled-smart > ${OUTPUT_DIR}/bench.union-lookup.scaled-smart

# Analyze
#${PISA_BIN}/query -i "${BASENAME}.yml" -q <(jq 'del(.threshold)' ${FILTERED_QUERIES} -c) --analyze --algorithm maxscore > ${OUTPUT_DIR}/stats.maxscore
#${PISA_BIN}/query -i "${BASENAME}.yml" -q ${FILTERED_QUERIES} --analyze --algorithm maxscore \
#    > ${OUTPUT_DIR}/stats.maxscore-thresholds
#${PISA_BIN}/query -i "${BASENAME}.yml" -q ${FILTERED_QUERIES} --analyze --algorithm maxscore-union-lookup \
#    > ${OUTPUT_DIR}/stats.maxscore-union-lookup
#${PISA_BIN}/query -i "${BASENAME}.yml" -q ${FILTERED_QUERIES} --analyze --algorithm unigram-union-lookup \
#    --intersections ${OUTPUT_DIR}/selections.1 > ${OUTPUT_DIR}/stats.unigram-union-lookup
#${PISA_BIN}/query -i "${BASENAME}.yml" -q ${FILTERED_QUERIES} --analyze --algorithm union-lookup \
#    --intersections ${OUTPUT_DIR}/selections.2 > ${OUTPUT_DIR}/stats.union-lookup.2
#${PISA_BIN}/query -i "${BASENAME}.yml" -q ${FILTERED_QUERIES} --analyze --algorithm lookup-union \
#    --intersections ${OUTPUT_DIR}/selections.2 > ${OUTPUT_DIR}/stats.lookup-union
#${PISA_BIN}/query -i "${BASENAME}.yml" -q ${FILTERED_QUERIES} --analyze --algorithm union-lookup \
#    --intersections ${OUTPUT_DIR}/selections.2.scaled-1.5 > ${OUTPUT_DIR}/stats.union-lookup.scaled-1.5
#${PISA_BIN}/query -i "${BASENAME}.yml" -q ${FILTERED_QUERIES} --analyze --algorithm union-lookup \
#    --intersections ${OUTPUT_DIR}/selections.2.scaled-2 > ${OUTPUT_DIR}/stats.union-lookup.scaled-2
#${PISA_BIN}/query -i "${BASENAME}.yml" -q ${FILTERED_QUERIES} --analyze --algorithm union-lookup \
#    --intersections ${OUTPUT_DIR}/selections.2.scaled-3 > ${OUTPUT_DIR}/stats.union-lookup.scaled-3
#${PISA_BIN}/query -i "${BASENAME}.yml" -q ${FILTERED_QUERIES} --analyze --algorithm union-lookup \
#    --intersections ${OUTPUT_DIR}/selections.2.scaled-smart > ${OUTPUT_DIR}/stats.union-lookup.scaled-smart

## Evaluate
#${PISA_BIN}/query -i "${BASENAME}.yml" -q <(jq 'del(.threshold)' ${FILTERED_QUERIES} -c) --algorithm maxscore > "${OUTPUT_DIR}/eval.maxscore"
#${PISA_BIN}/query -i "${BASENAME}.yml" -q ${FILTERED_QUERIES} --algorithm maxscore \
#    > "${OUTPUT_DIR}/eval.maxscore-threshold"
#${PISA_BIN}/query -i "${BASENAME}.yml" -q ${FILTERED_QUERIES} --algorithm maxscore-union-lookup \
#    > "${OUTPUT_DIR}/eval.maxscore-union-lookup"
#${PISA_BIN}/query -i "${BASENAME}.yml" -q ${FILTERED_QUERIES} --algorithm unigram-union-lookup \
#    --intersections ${OUTPUT_DIR}/selections.1 > "${OUTPUT_DIR}/eval.unigram-union-lookup"
#${PISA_BIN}/query -i "${BASENAME}.yml" -q ${FILTERED_QUERIES} --algorithm union-lookup \
#    --intersections ${OUTPUT_DIR}/selections.2 > "${OUTPUT_DIR}/eval.union-lookup.2"
#${PISA_BIN}/query -i "${BASENAME}.yml" -q ${FILTERED_QUERIES} --algorithm lookup-union \
#    --intersections ${OUTPUT_DIR}/selections.2 > "${OUTPUT_DIR}/eval.lookup-union"
#${PISA_BIN}/query -i "${BASENAME}.yml" -q ${FILTERED_QUERIES} --algorithm union-lookup \
#    --intersections ${OUTPUT_DIR}/selections.2.scaled-1.5 > "${OUTPUT_DIR}/eval.union-lookup.scale-1.5"
#${PISA_BIN}/query -i "${BASENAME}.yml" -q ${FILTERED_QUERIES} --algorithm union-lookup \
#    --intersections ${OUTPUT_DIR}/selections.2.scaled-2 > "${OUTPUT_DIR}/eval.union-lookup.scale-2"
#${PISA_BIN}/query -i "${BASENAME}.yml" -q ${FILTERED_QUERIES} --algorithm union-lookup \
#    --intersections ${OUTPUT_DIR}/selections.2.scaled-3 > "${OUTPUT_DIR}/eval.union-lookup.scale-3"
#${PISA_BIN}/query -i "${BASENAME}.yml" -q ${FILTERED_QUERIES} --algorithm union-lookup \
#    --intersections ${OUTPUT_DIR}/selections.2.scaled-smart > "${OUTPUT_DIR}/eval.union-lookup.scale-smart"
