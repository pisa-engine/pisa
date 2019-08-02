#include "cursor/scored_cursor.hpp"
#include "block_freq_index.hpp"
#include "codec/block_codecs.hpp"
#include "codec/list.hpp"
#include "codec/maskedvbyte.hpp"
#include "codec/qmx.hpp"
#include "codec/simdbp.hpp"
#include "codec/simple16.hpp"
#include "codec/simple8b.hpp"
#include "codec/streamvbyte.hpp"
#include "codec/varintgb.hpp"
#include "mixed_block.hpp"

namespace pisa {

#define PISA_SCORED_CURSOR(SCORER, CODEC, WAND)                                                  \
    template struct scored_cursor<block_freq_index<CODEC, false>,                                \
                                  typename scorer_traits<SCORER<wand_data<WAND>>>::term_scorer>; \
    template auto make_scored_cursors<block_freq_index<CODEC, false>, SCORER<wand_data<WAND>>>(  \
        block_freq_index<CODEC, false> const &, SCORER<wand_data<WAND>> const &, Query);

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
BOOST_PP_SEQ_FOR_EACH(LOOP_BODY, _, PISA_BLOCK_CODEC_TYPES);
#undef LOOP_BODY

} // namespace pisa
