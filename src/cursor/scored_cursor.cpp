#include "cursor/scored_cursor.hpp"
#include "block_freq_index.hpp"
#include "freq_index.hpp"
#include "index_types.hpp"

namespace pisa {

#define PISA_SCORED_CURSOR(SCORER, INDEX, WAND)                                              \
    template scored_cursor<                                                                  \
        BOOST_PP_CAT(INDEX, _index),                                                         \
        typename scorer_traits<SCORER<wand_data<WAND>>>::term_scorer>::~scored_cursor();     \
    template auto make_scored_cursors<BOOST_PP_CAT(INDEX, _index), SCORER<wand_data<WAND>>>( \
        BOOST_PP_CAT(INDEX, _index) const &, SCORER<wand_data<WAND>> const &, Query);

#define LOOP_BODY(R, DATA, T)                         \
    PISA_SCORED_CURSOR(bm25, T, wand_data_raw)        \
    PISA_SCORED_CURSOR(dph, T, wand_data_raw)         \
    PISA_SCORED_CURSOR(pl2, T, wand_data_raw)         \
    PISA_SCORED_CURSOR(qld, T, wand_data_raw)         \
    PISA_SCORED_CURSOR(bm25, T, wand_data_compressed) \
    PISA_SCORED_CURSOR(dph, T, wand_data_compressed)  \
    PISA_SCORED_CURSOR(pl2, T, wand_data_compressed)  \
    PISA_SCORED_CURSOR(qld, T, wand_data_compressed)  \
/**/
BOOST_PP_SEQ_FOR_EACH(LOOP_BODY, _, PISA_INDEX_TYPES);
#undef LOOP_BODY

} // namespace pisa
