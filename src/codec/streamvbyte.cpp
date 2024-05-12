#include <array>

#include "codec/streamvbyte.hpp"
#include "streamvbyte/include/streamvbyte.h"

namespace pisa {

void StreamVByteBlockCodec::encode(
    uint32_t const* in, uint32_t /* sum_of_values */, size_t n, std::vector<uint8_t>& out
) const {
    assert(n <= m_block_size);
    auto* src = const_cast<uint32_t*>(in);
    thread_local std::array<std::uint8_t, m_max_compressed_bytes> buf{};
    size_t out_len = streamvbyte_encode(src, n, buf.data());
    out.insert(out.end(), buf.data(), buf.data() + out_len);
}
uint8_t const* StreamVByteBlockCodec::decode(
    uint8_t const* in, uint32_t* out, uint32_t /* sum_of_values */, size_t n
) const {
    assert(n <= m_block_size);
    auto read = streamvbyte_decode(in, out, n);
    return in + read;
}

}  // namespace pisa
