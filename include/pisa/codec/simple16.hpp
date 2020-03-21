#pragma once
#include "FastPFor/headers/simple16.h"

namespace pisa {

struct simple16_block {
    static const uint64_t block_size = 128;

    static void
    encode(uint32_t const* in, uint32_t /* sum_of_values */, size_t n, std::vector<uint8_t>& out)
    {
        assert(n <= block_size);
        thread_local FastPForLib::Simple16<false> codec;
        thread_local std::vector<uint8_t> buf(2 * 8 * block_size);
        size_t out_len = buf.size();
        codec.encodeArray(in, n, reinterpret_cast<uint32_t*>(buf.data()), out_len);
        out_len *= 4;
        out.insert(out.end(), buf.data(), buf.data() + out_len);
    }

    static uint8_t const*
    decode(uint8_t const* in, uint32_t* out, uint32_t /* sum_of_values */, size_t n)
    {
        assert(n <= block_size);
        FastPForLib::Simple16<false> codec;
        std::vector<uint32_t> buf(2 * block_size);

        auto const* ret = reinterpret_cast<uint8_t const*>(
            codec.decodeArray(reinterpret_cast<uint32_t const*>(in), 8 * n, buf.data(), n));
        for (size_t i = 0; i < n; ++i) {
            *out++ = buf[i];
        }
        return ret;
    }
};
}  // namespace pisa
