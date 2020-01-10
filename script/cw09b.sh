# Fail if any variable is unset
set -u
set -e

source $1

echo "Running experiment with the following environment:"
echo ""
echo "  PISA_BIN         = ${PISA_BIN}"
echo "  INTERSECT_BIN    = ${INTERSECT_BIN}"
echo "  BINARY_FREQ_COLL = ${BINARY_FREQ_COLL}"
echo "  FWD              = ${FWD}"
echo "  BASENAME         = ${BASENAME}"
echo "  THREADS          = ${THREADS}"
echo "  ENCODING         = ${ENCODING}"
echo "  OUTPUT_DIR       = ${OUTPUT_DIR}"
echo "  QUERIES          = ${QUERIES}"
echo "  FILTERED_QUERIES = ${FILTERED_QUERIES}"
echo "  K                = ${K}"
echo "  THRESHOLDS       = ${THRESHOLDS}"
echo ""

set -x
mkdir -p ${OUTPUT_DIR}

## Compress an inverted index in `binary_freq_collection` format.
#${PISA_BIN}/compress -i ${BINARY_FREQ_COLL} --fwd ${FWD} -o ${BASENAME} -j ${THREADS} -e ${ENCODING}

# This will produce both quantized scores and max scores (both quantized and not).
#${PISA_BIN}/score -i "${BASENAME}.yml" -j ${THREADS}

# This will produce both quantized scores and max scores (both quantized and not).
#${PISA_BIN}/bmscore -i "${BASENAME}.yml" -j ${THREADS} --block-size 128

# Filter out queries witout existing terms.
paste -d: ${QUERIES} ${THRESHOLDS} \
    | jq '{"id": split(":")[0], "query": split(":")[1], "threshold": split(":")[2] | tonumber}' -R -c \
    | ${PISA_BIN}/filter-queries -i ${BASENAME}.yml > ${FILTERED_QUERIES}

# This will produce both quantized scores and max scores (both quantized and not).
${PISA_BIN}/bigram-index -i "${BASENAME}.yml" -q ${FILTERED_QUERIES}

# Extract intersections
${PISA_BIN}/intersection -i "${BASENAME}.yml" -q ${FILTERED_QUERIES} --combinations --existing \
    | grep -v "\[warning\]" \
    > ${OUTPUT_DIR}/intersections.jl

# Select unigrams
${INTERSECT_BIN} -m unigram ${OUTPUT_DIR}/intersections.jl > ${OUTPUT_DIR}/selections.1
${INTERSECT_BIN} -m bigram ${OUTPUT_DIR}/intersections.jl > ${OUTPUT_DIR}/selections.2
${INTERSECT_BIN} -m bigram ${OUTPUT_DIR}/intersections.jl \
    --scale 1.5 > ${OUTPUT_DIR}/selections.2.scaled-1.5
${INTERSECT_BIN} -m bigram ${OUTPUT_DIR}/intersections.jl \
    --scale 2 > ${OUTPUT_DIR}/selections.2.scaled-2

# Run benchmarks
#${PISA_BIN}/query -i "${BASENAME}.yml" -q <(jq 'del(.threshold)' ${FILTERED_QUERIES} -c) --benchmark --algorithm wand --safe > ${OUTPUT_DIR}/bench.wand
#${PISA_BIN}/query -i "${BASENAME}.yml" -q <(jq 'del(.threshold)' ${FILTERED_QUERIES} -c) --benchmark --algorithm bmw --safe > ${OUTPUT_DIR}/bench.bmw
#${PISA_BIN}/query -i "${BASENAME}.yml" -q <(jq 'del(.threshold)' ${FILTERED_QUERIES} -c) --benchmark --algorithm maxscore --safe > ${OUTPUT_DIR}/bench.maxscore
#${PISA_BIN}/query -i "${BASENAME}.yml" -q ${FILTERED_QUERIES} --benchmark --algorithm bmw --safe \
#    > ${OUTPUT_DIR}/bench.bmw-threshold
${PISA_BIN}/query -i "${BASENAME}.yml" -q ${FILTERED_QUERIES} --benchmark --algorithm maxscore --safe \
    > ${OUTPUT_DIR}/bench.maxscore-threshold
#${PISA_BIN}/query -i "${BASENAME}.yml" -q ${FILTERED_QUERIES} --benchmark --algorithm maxscore-union-lookup --safe \
#    > ${OUTPUT_DIR}/bench.maxscore-union-lookup
${PISA_BIN}/query -i "${BASENAME}.yml" -q ${OUTPUT_DIR}/selections.1 --benchmark --algorithm unigram-union-lookup --safe \
    > ${OUTPUT_DIR}/bench.unigram-union-lookup
#${PISA_BIN}/query -i "${BASENAME}.yml" -q ${OUTPUT_DIR}/selections.2 --benchmark --algorithm union-lookup --safe \
#    > ${OUTPUT_DIR}/bench.union-lookup
${PISA_BIN}/query -i "${BASENAME}.yml" -q ${OUTPUT_DIR}/selections.2 --benchmark --algorithm lookup-union --safe \
    > ${OUTPUT_DIR}/bench.lookup-union
#${PISA_BIN}/query -i "${BASENAME}.yml" -q ${OUTPUT_DIR}/selections.2.scaled-1.5 --safe \
#    --benchmark --algorithm lookup-union \
#    > ${OUTPUT_DIR}/bench.lookup-union.scaled-1.5
#${PISA_BIN}/query -i "${BASENAME}.yml" -q ${OUTPUT_DIR}/selections.2.scaled-2 --safe \
#    --benchmark --algorithm lookup-union \
#    > ${OUTPUT_DIR}/bench.lookup-union.scaled-2

# Analyze
# ${PISA_BIN}/query -i "${BASENAME}.yml" -q <(jq 'del(.threshold)' ${FILTERED_QUERIES} -c) --inspect --algorithm maxscore > ${OUTPUT_DIR}/stats.maxscore
# ${PISA_BIN}/query -i "${BASENAME}.yml" -q ${FILTERED_QUERIES} --inspect --algorithm maxscore \
#     > ${OUTPUT_DIR}/stats.maxscore-threshold
# ${PISA_BIN}/query -i "${BASENAME}.yml" -q ${FILTERED_QUERIES} --inspect --algorithm maxscore-union-lookup \
#     > ${OUTPUT_DIR}/stats.maxscore-union-lookup
${PISA_BIN}/query -i "${BASENAME}.yml" -q ${OUTPUT_DIR}/selections.1 --inspect --algorithm unigram-union-lookup \
    > ${OUTPUT_DIR}/stats.unigram-union-lookup
# ${PISA_BIN}/query -i "${BASENAME}.yml" -q ${OUTPUT_DIR}/selections.2 --inspect --algorithm union-lookup \
#     > ${OUTPUT_DIR}/stats.union-lookup
${PISA_BIN}/query -i "${BASENAME}.yml" -q ${OUTPUT_DIR}/selections.2 --inspect --algorithm lookup-union \
    > ${OUTPUT_DIR}/stats.lookup-union
# ${PISA_BIN}/query -i "${BASENAME}.yml" -q ${OUTPUT_DIR}/selections.2.scaled-1.5 \
#     --inspect --algorithm lookup-union \
#     > ${OUTPUT_DIR}/stats.lookup-union.scaled-1.5

# Evaluate
#${PISA_BIN}/query -i "${BASENAME}.yml" -q <(jq 'del(.threshold)' ${FILTERED_QUERIES} -c) --algorithm maxscore > "${OUTPUT_DIR}/eval.maxscore"
#${PISA_BIN}/query -i "${BASENAME}.yml" -q ${FILTERED_QUERIES} --algorithm maxscore \
#    > "${OUTPUT_DIR}/eval.maxscore-threshold"
#${PISA_BIN}/query -i "${BASENAME}.yml" -q ${FILTERED_QUERIES} --algorithm maxscore-union-lookup \
#    > "${OUTPUT_DIR}/eval.maxscore-union-lookup"
#${PISA_BIN}/query -i "${BASENAME}.yml" -q ${OUTPUT_DIR}/selections.1 --algorithm unigram-union-lookup \
#    > "${OUTPUT_DIR}/eval.unigram-union-lookup"
#${PISA_BIN}/query -i "${BASENAME}.yml" -q ${OUTPUT_DIR}/selections.2 --algorithm union-lookup \
#    > "${OUTPUT_DIR}/eval.union-lookup"
#${PISA_BIN}/query -i "${BASENAME}.yml" -q ${OUTPUT_DIR}/selections.2 --algorithm lookup-union \
#    > "${OUTPUT_DIR}/eval.lookup-union"
#${PISA_BIN}/query -i "${BASENAME}.yml" -q ${OUTPUT_DIR}/selections.2.scaled-1.5 --algorithm lookup-union \
#    > "${OUTPUT_DIR}/eval.lookup-union.scaled-1.5"
