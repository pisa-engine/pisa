#pragma once

#include "codec/block_codecs.hpp"
#include "util/util.hpp"
#include <vector>

extern "C" {
#include "simdcomp/include/simdbitpacking.h"
}

namespace pisa {
struct simdbp_block {
    static const uint64_t block_size = 128;
    static void encode(uint32_t const* in, uint32_t sum_of_values, size_t n, std::vector<uint8_t>& out)
    {
        assert(n <= block_size);
        auto* src = const_cast<uint32_t*>(in);
        if (n < block_size) {
            interpolative_block::encode(src, sum_of_values, n, out);
            return;
        }
        uint32_t b = maxbits(in);
        thread_local std::vector<uint8_t> buf(8 * n);
        uint8_t* buf_ptr = buf.data();
        *buf_ptr++ = b;
        simdpackwithoutmask(src, (__m128i*)buf_ptr, b);
        out.insert(out.end(), buf.data(), buf.data() + b * sizeof(__m128i) + 1);
    }
    static uint8_t const* decode(uint8_t const* in, uint32_t* out, uint32_t sum_of_values, size_t n)
    {
        assert(n <= block_size);
        if (PISA_UNLIKELY(n < block_size)) {
            return interpolative_block::decode(in, out, sum_of_values, n);
        }
        uint32_t b = *in++;
        simdunpack((const __m128i*)in, out, b);
        return in + b * sizeof(__m128i);
    }
};
}  // namespace pisa
