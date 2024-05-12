#include <array>

#include "FastPFor/headers/simple8b.h"
#include "codec/simple8b.hpp"

namespace pisa {

void Simple8bBlockCodec::encode(
    uint32_t const* in, uint32_t sum_of_values, size_t n, std::vector<uint8_t>& out
) const {
    assert(n <= m_block_size);
    thread_local FastPForLib::Simple8b<false> codec;
    thread_local std::array<std::uint8_t, 2 * 8 * m_block_size> buf{};
    size_t out_len = buf.size();
    codec.encodeArray(in, n, reinterpret_cast<uint32_t*>(buf.data()), out_len);
    out_len *= 4;
    out.insert(out.end(), buf.data(), buf.data() + out_len);
}

uint8_t const*
Simple8bBlockCodec::decode(uint8_t const* in, uint32_t* out, uint32_t sum_of_values, size_t n) const {
    assert(n <= m_block_size);
    FastPForLib::Simple8b<false> codec;
    return reinterpret_cast<uint8_t const*>(
        codec.decodeArray(reinterpret_cast<uint32_t const*>(in), 8 * n, out, n)
    );
}

}  // namespace pisa
