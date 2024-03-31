#include "codec/maskedvbyte.hpp"

namespace pisa {

void MaskedVByteBlockCodec::encode(
    uint32_t const* in, uint32_t sum_of_values, size_t n, std::vector<uint8_t>& out
) const {
    assert(n <= m_block_size);
    auto* src = const_cast<uint32_t*>(in);
    if (n < m_block_size) {
        interpolative_block::encode(src, sum_of_values, n, out);
        return;
    }
    thread_local std::array<std::uint8_t, 2 * m_block_size * sizeof(std::uint32_t)> buf{};
    size_t out_len = vbyte_encode(src, n, buf.data());
    out.insert(out.end(), buf.data(), buf.data() + out_len);
}

uint8_t const*
MaskedVByteBlockCodec::decode(uint8_t const* in, uint32_t* out, uint32_t sum_of_values, size_t n) const {
    assert(n <= m_block_size);
    if PISA_UNLIKELY (n < m_block_size) {
        return interpolative_block::decode(in, out, sum_of_values, n);
    }
    auto read = masked_vbyte_decode(in, out, n);
    return in + read;
}

}  // namespace pisa
