#include "cursor/scored_cursor.hpp"
#include "macro.hpp"
#include "query/algorithm/ranked_or_query.hpp"

namespace pisa {

#define LOOP_BODY(R, DATA, T)                                           \
    PISA_DAAT_ALGORITHM(ranked_or_query, bm25, T, wand_data_raw)        \
    PISA_DAAT_ALGORITHM(ranked_or_query, dph, T, wand_data_raw)         \
    PISA_DAAT_ALGORITHM(ranked_or_query, pl2, T, wand_data_raw)         \
    PISA_DAAT_ALGORITHM(ranked_or_query, qld, T, wand_data_raw)         \
    PISA_DAAT_ALGORITHM(ranked_or_query, bm25, T, wand_data_compressed) \
    PISA_DAAT_ALGORITHM(ranked_or_query, dph, T, wand_data_compressed)  \
    PISA_DAAT_ALGORITHM(ranked_or_query, pl2, T, wand_data_compressed)  \
    PISA_DAAT_ALGORITHM(ranked_or_query, qld, T, wand_data_compressed)
/**/
BOOST_PP_SEQ_FOR_EACH(LOOP_BODY, _, PISA_BLOCK_CODEC_TYPES);
#undef LOOP_BODY

} // namespace pisa
