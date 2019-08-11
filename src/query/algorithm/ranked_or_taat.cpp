#include "accumulator/lazy_accumulator.hpp"
#include "accumulator/simple_accumulator.hpp"
#include "cursor/scored_cursor.hpp"
#include "macro.hpp"
#include "query/algorithm/ranked_or_taat_query.hpp"

namespace pisa {

#define LOOP_BODY(R, DATA, T)                                                                    \
    PISA_TAAT_ALGORITHM(ranked_or_taat_query, bm25, T, wand_data_raw, SimpleAccumulator)         \
    PISA_TAAT_ALGORITHM(ranked_or_taat_query, dph, T, wand_data_raw, SimpleAccumulator)          \
    PISA_TAAT_ALGORITHM(ranked_or_taat_query, pl2, T, wand_data_raw, SimpleAccumulator)          \
    PISA_TAAT_ALGORITHM(ranked_or_taat_query, qld, T, wand_data_raw, SimpleAccumulator)          \
    PISA_TAAT_ALGORITHM(ranked_or_taat_query, bm25, T, wand_data_compressed, SimpleAccumulator)  \
    PISA_TAAT_ALGORITHM(ranked_or_taat_query, dph, T, wand_data_compressed, SimpleAccumulator)   \
    PISA_TAAT_ALGORITHM(ranked_or_taat_query, pl2, T, wand_data_compressed, SimpleAccumulator)   \
    PISA_TAAT_ALGORITHM(ranked_or_taat_query, qld, T, wand_data_compressed, SimpleAccumulator)   \
    PISA_TAAT_ALGORITHM(ranked_or_taat_query, bm25, T, wand_data_raw, LazyAccumulator<4>)        \
    PISA_TAAT_ALGORITHM(ranked_or_taat_query, dph, T, wand_data_raw, LazyAccumulator<4>)         \
    PISA_TAAT_ALGORITHM(ranked_or_taat_query, pl2, T, wand_data_raw, LazyAccumulator<4>)         \
    PISA_TAAT_ALGORITHM(ranked_or_taat_query, qld, T, wand_data_raw, LazyAccumulator<4>)         \
    PISA_TAAT_ALGORITHM(ranked_or_taat_query, bm25, T, wand_data_compressed, LazyAccumulator<4>) \
    PISA_TAAT_ALGORITHM(ranked_or_taat_query, dph, T, wand_data_compressed, LazyAccumulator<4>)  \
    PISA_TAAT_ALGORITHM(ranked_or_taat_query, pl2, T, wand_data_compressed, LazyAccumulator<4>)  \
    PISA_TAAT_ALGORITHM(ranked_or_taat_query, qld, T, wand_data_compressed, LazyAccumulator<4>)
/**/
BOOST_PP_SEQ_FOR_EACH(LOOP_BODY, _, PISA_INDEX_TYPES);
#undef LOOP_BODY

} // namespace pisa
