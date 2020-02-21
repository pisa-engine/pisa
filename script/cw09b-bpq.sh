PISA_BIN="/home/michal/pisa/build/bin"
INTERSECT_BIN="/home/michal/intersect/target/release/intersect"
BINARY_FREQ_COLL="/home/amallia/cw09b/CW09B.inv.bp"
FWD="/home/amallia/cw09b/CW09B.fwd.bp"
ENCODING="simdbp" # v1
BASENAME="/data/michal/work/v1/cw09b-bp/cw09b-${ENCODING}"
THREADS=4
QUERIES="/home/michal/05.clean.shuf.test"
K=1000
OUTPUT_DIR="/data/michal/intersect/cw09b-bp"
FILTERED_QUERIES="${OUTPUT_DIR}/$(basename ${QUERIES}).filtered"
#PAIRS="/home/michal/real.aol.top50k.jl"
PAIRS=${FILTERED_QUERIES}
PAIR_INDEX_BASENAME="${BASENAME}-pair"
THRESHOLDS="/home/michal/biscorer/data/thresholds/cw09b/thresholds.cw09b.0_01.top20.bm25.05.clean.shuf.test"
QUERY_LIMIT=1000

# 134384  149026  1648    128376  4 # UL
# 149026  149026  1648    109040  4 # 15542 LU
# 2261867 2308897 1648     94280  3 # 44935 MS
# T = 12.985799789428713
