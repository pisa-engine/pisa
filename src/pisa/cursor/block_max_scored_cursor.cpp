#include "cursor/block_max_scored_cursor.hpp"
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
#include "scorer/bm25.hpp"
#include "wand_data.hpp"
#include "wand_data_raw.hpp"

namespace pisa {

#define LOOP_BODY(R, DATA, T)                                                          \
    template auto make_block_max_scored_cursors<block_freq_index<T, false>,            \
                                                wand_data<bm25, wand_data_raw<bm25>>>( \
        block_freq_index<T, false> const &, wand_data<bm25, wand_data_raw<bm25>> const &, Query);
/**/
BOOST_PP_SEQ_FOR_EACH(LOOP_BODY, _, PISA_BLOCK_CODEC_TYPES);
#undef LOOP_BODY

} // namespace pisa
