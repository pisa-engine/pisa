#include "codec/simple16.hpp"

namespace pisa {

void Simple16BlockCodec::encode(
    uint32_t const* in, [[maybe_unused]] uint32_t sum_of_values, size_t n, std::vector<uint8_t>& out
) const {
    assert(n <= m_block_size);
    thread_local FastPForLib::Simple16<false> codec;
    thread_local std::array<std::uint8_t, 2 * 8 * m_block_size> buf{};
    size_t out_len = buf.size();
    codec.encodeArray(in, n, reinterpret_cast<uint32_t*>(buf.data()), out_len);
    out_len *= 4;
    out.insert(out.end(), buf.data(), buf.data() + out_len);
}

uint8_t const*
Simple16BlockCodec::decode(uint8_t const* in, uint32_t* out, uint32_t sum_of_values, size_t n) const {
    assert(n <= m_block_size);
    FastPForLib::Simple16<false> codec;
    std::array<std::uint32_t, 2 * m_block_size> buf{};

    auto const* ret = reinterpret_cast<uint8_t const*>(
        codec.decodeArray(reinterpret_cast<uint32_t const*>(in), 8 * n, buf.data(), n)
    );

    std::copy(buf.begin(), std::next(buf.begin(), n), out);
    return ret;
}

}  // namespace pisa
