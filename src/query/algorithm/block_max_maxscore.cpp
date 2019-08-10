#include "cursor/block_max_scored_cursor.hpp"
#include "index_types.hpp"
#include "macro.hpp"
#include "query/algorithm/block_max_maxscore_query.hpp"

namespace pisa {

#define LOOP_BODY(R, DATA, T)                                                              \
    PISA_DAAT_BLOCK_MAX_ALGORITHM(block_max_maxscore_query, bm25, T, wand_data_raw)        \
    PISA_DAAT_BLOCK_MAX_ALGORITHM(block_max_maxscore_query, dph, T, wand_data_raw)         \
    PISA_DAAT_BLOCK_MAX_ALGORITHM(block_max_maxscore_query, pl2, T, wand_data_raw)         \
    PISA_DAAT_BLOCK_MAX_ALGORITHM(block_max_maxscore_query, qld, T, wand_data_raw)         \
    PISA_DAAT_BLOCK_MAX_ALGORITHM(block_max_maxscore_query, bm25, T, wand_data_compressed) \
    PISA_DAAT_BLOCK_MAX_ALGORITHM(block_max_maxscore_query, dph, T, wand_data_compressed)  \
    PISA_DAAT_BLOCK_MAX_ALGORITHM(block_max_maxscore_query, pl2, T, wand_data_compressed)  \
    PISA_DAAT_BLOCK_MAX_ALGORITHM(block_max_maxscore_query, qld, T, wand_data_compressed)
/**/
BOOST_PP_SEQ_FOR_EACH(LOOP_BODY, _, PISA_INDEX_TYPES);
#undef LOOP_BODY

} // namespace pisa
