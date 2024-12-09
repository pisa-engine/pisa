#include <array>

#include "codec/block_codecs.hpp"
#include "codec/varintgb.hpp"

namespace pisa {

void VarintGbBlockCodec::encode(
    uint32_t const* in, uint32_t sum_of_values, size_t n, std::vector<uint8_t>& out
) const {
    thread_local VarIntGB<false> varintgb_codec;
    assert(n <= m_block_size);
    if (n < m_block_size) {
        interpolative_block::encode(in, sum_of_values, n, out);
        return;
    }
    thread_local std::array<std::uint8_t, 2 * m_block_size * sizeof(uint32_t)> buf{};
    size_t out_len = varintgb_codec.encodeArray(in, n, buf.data());
    out.insert(out.end(), buf.data(), buf.data() + out_len);
}

uint8_t const*
VarintGbBlockCodec::decode(uint8_t const* in, uint32_t* out, uint32_t sum_of_values, size_t n) const {
    thread_local VarIntGB<false> varintgb_codec;
    assert(n <= m_block_size);
    if (n < m_block_size) [[unlikely]] {
        return interpolative_block::decode(in, out, sum_of_values, n);
    }
    auto read = varintgb_codec.decodeArray(in, n, out);
    return read + in;
}

}  // namespace pisa
