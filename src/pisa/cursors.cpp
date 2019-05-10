#include "cursor/block_max_scored_cursor.hpp"
#include "cursor/cursor.hpp"
#include "cursor/scored_cursor.hpp"
#include "index_types.hpp"
#include "scorer/bm25.hpp"
#include "wand_data.hpp"

namespace pisa {

#define LOOP_BODY(R, DATA, T)                                                                    \
    template auto make_cursors<BOOST_PP_CAT(T, _index)>(BOOST_PP_CAT(T, _index) const &, Query); \
    template struct scored_cursor<BOOST_PP_CAT(T, _index), bm25>;                                \
    template auto make_scored_cursors<BOOST_PP_CAT(T, _index), wand_data<>>(                     \
        BOOST_PP_CAT(T, _index) const &, wand_data<> const &, Query query);                      \
    template struct block_max_scored_cursor<BOOST_PP_CAT(T, _index), wand_data<>, bm25>;         \
    template auto make_block_max_scored_cursors<BOOST_PP_CAT(T, _index), wand_data<>>(           \
        BOOST_PP_CAT(T, _index) const &, wand_data<> const &, Query query);                      \
/**/
BOOST_PP_SEQ_FOR_EACH(LOOP_BODY, _, PISA_INDEX_TYPES);
#undef LOOP_BODY
}
