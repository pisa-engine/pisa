#pragma once

#include <boost/preprocessor/cat.hpp>
#include <boost/preprocessor/seq/for_each.hpp>
#include <boost/preprocessor/stringize.hpp>

#include "codec/block_codecs.hpp"
#include "codec/maskedvbyte.hpp"
#include "codec/qmx.hpp"
#include "codec/varintgb.hpp"
#include "codec/streamvbyte.hpp"

#include "binary_freq_collection.hpp"
#include "block_freq_index.hpp"
#include "freq_index.hpp"
#include "mixed_block.hpp"
#include "sequence/partitioned_sequence.hpp"
#include "sequence/positive_sequence.hpp"
#include "sequence/uniform_partitioned_sequence.hpp"

namespace ds2i {
using ef_index = freq_index<compact_elias_fano, positive_sequence<strict_elias_fano>>;

using single_index = freq_index<indexed_sequence, positive_sequence<>>;

using uniform_index = freq_index<uniform_partitioned_sequence<>,
                                 positive_sequence<uniform_partitioned_sequence<strict_sequence>>>;

using opt_index =
    freq_index<partitioned_sequence<>, positive_sequence<partitioned_sequence<strict_sequence>>>;

using block_optpfor_index           = block_freq_index<ds2i::optpfor_block>;
using block_varintg8iu_index        = block_freq_index<ds2i::varint_G8IU_block>;
using block_streamvbyte_index       = block_freq_index<ds2i::streamvbyte_block>;
using block_maskedvbyte_index       = block_freq_index<ds2i::maskedvbyte_block>;
using block_varintgb_index               = block_freq_index<ds2i::varintgb_block>;
using block_interpolative_index     = block_freq_index<ds2i::interpolative_block>;
using block_qmx_index               = block_freq_index<ds2i::qmx_block>;
using block_mixed_index             = block_freq_index<ds2i::mixed_block>;

} // namespace ds2i

#define DS2I_INDEX_TYPES                                                            \
    (ef)(single)(uniform)(opt)(block_optpfor)(block_varintg8iu)(block_streamvbyte)( \
        block_maskedvbyte)(block_interpolative)(block_qmx)(block_varintgb)(block_mixed)
#define DS2I_BLOCK_INDEX_TYPES                                                                    \
    (block_optpfor)(block_varintg8iu)(block_streamvbyte)(block_maskedvbyte)(block_interpolative)( \
        block_qmx)(block_varintgb)(block_mixed)
