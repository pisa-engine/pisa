#include "cursor/scored_cursor.hpp"
#include "index_types.hpp"
#include "macro.hpp"
#include "query/algorithm/ranked_or_query.hpp"

namespace pisa {

#define PISA_RANKED_OR_EXECUTOR(SCORER, INDEX, WAND) \
    template QueryExecutor ranked_or_executor(       \
        BOOST_PP_CAT(INDEX, _index) const &, SCORER<wand_data<WAND>> const &, int);

#define LOOP_BODY(R, DATA, T)                                           \
    PISA_DAAT_ALGORITHM(ranked_or_query, bm25, T, wand_data_raw)        \
    PISA_DAAT_ALGORITHM(ranked_or_query, dph, T, wand_data_raw)         \
    PISA_DAAT_ALGORITHM(ranked_or_query, pl2, T, wand_data_raw)         \
    PISA_DAAT_ALGORITHM(ranked_or_query, qld, T, wand_data_raw)         \
    PISA_DAAT_ALGORITHM(ranked_or_query, bm25, T, wand_data_compressed) \
    PISA_DAAT_ALGORITHM(ranked_or_query, dph, T, wand_data_compressed)  \
    PISA_DAAT_ALGORITHM(ranked_or_query, pl2, T, wand_data_compressed)  \
    PISA_DAAT_ALGORITHM(ranked_or_query, qld, T, wand_data_compressed)  \
    PISA_RANKED_OR_EXECUTOR(bm25, T, wand_data_raw)                     \
    PISA_RANKED_OR_EXECUTOR(dph, T, wand_data_raw)                      \
    PISA_RANKED_OR_EXECUTOR(pl2, T, wand_data_raw)                      \
    PISA_RANKED_OR_EXECUTOR(qld, T, wand_data_raw)                      \
    PISA_RANKED_OR_EXECUTOR(bm25, T, wand_data_compressed)              \
    PISA_RANKED_OR_EXECUTOR(dph, T, wand_data_compressed)               \
    PISA_RANKED_OR_EXECUTOR(pl2, T, wand_data_compressed)               \
    PISA_RANKED_OR_EXECUTOR(qld, T, wand_data_compressed)               \
/**/
BOOST_PP_SEQ_FOR_EACH(LOOP_BODY, _, PISA_INDEX_TYPES);
#undef LOOP_BODY
#undef PISA_RANKED_OR_EXECUTOR

} // namespace pisa
