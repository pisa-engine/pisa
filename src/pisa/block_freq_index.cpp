#include "block_freq_index.hpp"
#include "codec/block_codecs.hpp"
#include "codec/maskedvbyte.hpp"
#include "codec/qmx.hpp"
#include "codec/simdbp.hpp"
#include "codec/simple16.hpp"
#include "codec/simple8b.hpp"
#include "codec/streamvbyte.hpp"
#include "codec/varintgb.hpp"
#include "mixed_block.hpp"

namespace pisa {

template class block_freq_index<pisa::optpfor_block>;
template class block_freq_index<pisa::varint_G8IU_block>;
template class block_freq_index<pisa::streamvbyte_block>;
template class block_freq_index<pisa::maskedvbyte_block>;
template class block_freq_index<pisa::varintgb_block>;
template class block_freq_index<pisa::interpolative_block>;
template class block_freq_index<pisa::qmx_block>;
template class block_freq_index<pisa::simple8b_block>;
template class block_freq_index<pisa::simple16_block>;
template class block_freq_index<pisa::simdbp_block>;
template class block_freq_index<pisa::mixed_block>;

} // namespace pisa
