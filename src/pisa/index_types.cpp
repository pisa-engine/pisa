#include "index_types.hpp"

namespace pisa {

using ef_index = freq_index<compact_elias_fano, positive_sequence<strict_elias_fano>>;
using single_index = freq_index<indexed_sequence, positive_sequence<>>;
using uniform_index = freq_index<uniform_partitioned_sequence<>,
                                 positive_sequence<uniform_partitioned_sequence<strict_sequence>>>;
using opt_index =
    freq_index<partitioned_sequence<>, positive_sequence<partitioned_sequence<strict_sequence>>>;

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
