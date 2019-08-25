#include "query/algorithm/block_max_ranked_and_query.hpp"
#include "cursor/scored_cursor.hpp"
#include "macro.hpp"

namespace pisa {

#define LOOP_BODY(R, DATA, T)                                                                \
    PISA_DAAT_BLOCK_MAX_ALGORITHM(block_max_ranked_and_query, bm25, T, wand_data_raw)        \
    PISA_DAAT_BLOCK_MAX_ALGORITHM(block_max_ranked_and_query, dph, T, wand_data_raw)         \
    PISA_DAAT_BLOCK_MAX_ALGORITHM(block_max_ranked_and_query, pl2, T, wand_data_raw)         \
    PISA_DAAT_BLOCK_MAX_ALGORITHM(block_max_ranked_and_query, qld, T, wand_data_raw)         \
    PISA_DAAT_BLOCK_MAX_ALGORITHM(block_max_ranked_and_query, bm25, T, wand_data_compressed) \
    PISA_DAAT_BLOCK_MAX_ALGORITHM(block_max_ranked_and_query, dph, T, wand_data_compressed)  \
    PISA_DAAT_BLOCK_MAX_ALGORITHM(block_max_ranked_and_query, pl2, T, wand_data_compressed)  \
    PISA_DAAT_BLOCK_MAX_ALGORITHM(block_max_ranked_and_query, qld, T, wand_data_compressed)  \
/**/
BOOST_PP_SEQ_FOR_EACH(LOOP_BODY, _, PISA_INDEX_TYPES);
#undef LOOP_BODY

} // namespace pisa
