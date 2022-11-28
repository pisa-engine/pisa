#pragma once
#include "FastPFor/headers/simple8b.h"

#include <array>

namespace pisa {

struct simple8b_block {
    static constexpr std::uint64_t block_size = 128;

    static void
    encode(uint32_t const* in, uint32_t /* sum_of_values */, size_t n, std::vector<uint8_t>& out)
    {
        assert(n <= block_size);
        thread_local FastPForLib::Simple8b<false> codec;
        thread_local std::array<std::uint8_t, 2 * 8 * block_size> buf{};
        size_t out_len = buf.size();
        codec.encodeArray(in, n, reinterpret_cast<uint32_t*>(buf.data()), out_len);
        out_len *= 4;
        out.insert(out.end(), buf.data(), buf.data() + out_len);
    }

    static uint8_t const*
    decode(uint8_t const* in, uint32_t* out, uint32_t /* sum_of_values */, size_t n)
    {
        assert(n <= block_size);
        FastPForLib::Simple8b<false> codec;
        return reinterpret_cast<uint8_t const*>(
            codec.decodeArray(reinterpret_cast<uint32_t const*>(in), 8 * n, out, n));
    }
};
}  // namespace pisa
