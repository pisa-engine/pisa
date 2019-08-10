#include "cursor/block_max_scored_cursor.hpp"
#include "index_types.hpp"

namespace pisa {

#define PISA_BLOCK_MAX_SCORED_CURSOR(SCORER, INDEX, WAND)                                          \
    template block_max_scored_cursor<                                                              \
        BOOST_PP_CAT(INDEX, _index),                                                               \
        wand_data<WAND>,                                                                           \
        typename scorer_traits<SCORER<wand_data<WAND>>>::term_scorer>::~block_max_scored_cursor(); \
    template auto make_block_max_scored_cursors<BOOST_PP_CAT(INDEX, _index),                       \
                                                wand_data<WAND>,                                   \
                                                SCORER<wand_data<WAND>>>(                          \
        BOOST_PP_CAT(INDEX, _index) const &,                                                       \
        wand_data<WAND> const &,                                                                   \
        SCORER<wand_data<WAND>> const &,                                                           \
        Query);

#define LOOP_BODY(R, DATA, T)                                          \
    struct T;                                                          \
    PISA_BLOCK_MAX_SCORED_CURSOR(bm25, T, wand_data_raw)        \
    PISA_BLOCK_MAX_SCORED_CURSOR(dph, T, wand_data_raw)         \
    PISA_BLOCK_MAX_SCORED_CURSOR(pl2, T, wand_data_raw)         \
    PISA_BLOCK_MAX_SCORED_CURSOR(qld, T, wand_data_raw)         \
    PISA_BLOCK_MAX_SCORED_CURSOR(bm25, T, wand_data_compressed) \
    PISA_BLOCK_MAX_SCORED_CURSOR(dph, T, wand_data_compressed)  \
    PISA_BLOCK_MAX_SCORED_CURSOR(pl2, T, wand_data_compressed)  \
    PISA_BLOCK_MAX_SCORED_CURSOR(qld, T, wand_data_compressed)  \
/**/
BOOST_PP_SEQ_FOR_EACH(LOOP_BODY, _, PISA_INDEX_TYPES);
#undef LOOP_BODY

} // namespace pisa
