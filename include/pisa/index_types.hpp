#pragma once

#include "boost/preprocessor/cat.hpp"
#include "boost/preprocessor/seq/for_each.hpp"
#include "boost/preprocessor/stringize.hpp"

#include "codec/block_codecs.hpp"
#include "codec/maskedvbyte.hpp"
#include "codec/qmx.hpp"
#include "codec/simdbp.hpp"
#include "codec/simple16.hpp"
#include "codec/simple8b.hpp"
#include "codec/streamvbyte.hpp"
#include "codec/varintgb.hpp"

#include "binary_freq_collection.hpp"
#include "block_freq_index.hpp"

#include "freq_index.hpp"
#include "mixed_block.hpp"
#include "sequence/partitioned_sequence.hpp"
#include "sequence/positive_sequence.hpp"
#include "sequence/uniform_partitioned_sequence.hpp"

namespace pisa {
using ef_index = freq_index<compact_elias_fano, positive_sequence<strict_elias_fano>>;

using single_index = freq_index<indexed_sequence, positive_sequence<>>;

using uniform_index = freq_index<uniform_partitioned_sequence<>,
                                 positive_sequence<uniform_partitioned_sequence<strict_sequence>>>;

using opt_index =
    freq_index<partitioned_sequence<>, positive_sequence<partitioned_sequence<strict_sequence>>>;

} // namespace pisa

#define PISA_INDEX_TYPES (ef)(single)(uniform)(opt)

#define PISA_BLOCK_CODEC_TYPES                                                               \
    (optpfor)(varint_G8IU)(streamvbyte)(maskedvbyte)(interpolative)(qmx)(varintgb)(simple8b)( \
        simple16)(simdbp)(mixed)
