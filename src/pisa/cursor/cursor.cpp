#include "cursor/cursor.hpp"
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

#define LOOP_BODY(R, DATA, T)                                                                  \
    template auto make_cursors<block_freq_index<T, false>>(block_freq_index<T, false> const &, \
                                                           Query);
/**/
BOOST_PP_SEQ_FOR_EACH(LOOP_BODY, _, PISA_BLOCK_CODEC_TYPES);
#undef LOOP_BODY

} // namespace pisa
