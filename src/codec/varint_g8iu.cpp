#include <cassert>

#include "codec/VarIntG8IU.h"
#include "codec/block_codecs.hpp"
#include "codec/varint_g8iu.hpp"

namespace pisa {

struct Codec: VarIntG8IU {
    // rewritten version of decodeBlock optimized for when the output
    // size is known rather than the input
    // the buffers pointed by src and dst must be respectively at least
    // 9 and 8 elements large
    uint32_t decodeBlock(uint8_t const*& src, uint32_t* dst) const {
        uint8_t desc = *src;
        src += 1;
        const __m128i data = _mm_lddqu_si128(reinterpret_cast<__m128i const*>(src));
        src += 8;
        const __m128i result = _mm_shuffle_epi8(data, vecmask[desc][0]);
        _mm_storeu_si128(reinterpret_cast<__m128i*>(dst), result);
        int readSize = maskOutputSize[desc];

        if (readSize > 4) {
            const __m128i result2 =
                _mm_shuffle_epi8(data, vecmask[desc][1]);  //__builtin_ia32_pshufb128(data,
                                                           // shf2);
            _mm_storeu_si128(reinterpret_cast<__m128i*>(dst + 4), result2);
            //__builtin_ia32_storedqu(dst
            //+ (16), result2);
        }

        return readSize;
    }
};

void VarintG8IUBlockCodec::encode(
    uint32_t const* in, uint32_t sum_of_values, size_t n, std::vector<uint8_t>& out
) const {
    thread_local Codec varint_codec;
    thread_local std::array<std::uint8_t, 2 * 4 * m_block_size> buf{};
    assert(n <= m_block_size);

    if (n < m_block_size) {
        interpolative_block::encode(in, sum_of_values, n, out);
        return;
    }

    size_t out_len = buf.size();

    const uint32_t* src = in;
    unsigned char* dst = buf.data();
    size_t srclen = n * 4;
    size_t dstlen = out_len;
    out_len = 0;
    while (srclen > 0 && dstlen >= 9) {
        out_len += varint_codec.encodeBlock(src, srclen, dst, dstlen);
    }
    assert(srclen == 0);
    out.insert(out.end(), buf.data(), buf.data() + out_len);
}

uint8_t const*
VarintG8IUBlockCodec::decode(uint8_t const* in, uint32_t* out, uint32_t sum_of_values, size_t n) const {
    static Codec varint_codec;  // decodeBlock is thread-safe
    assert(n <= m_block_size);

    if (n < m_block_size) [[unlikely]] {
        return interpolative_block::decode(in, out, sum_of_values, n);
    }

    size_t out_len = 0;
    uint8_t const* src = in;
    uint32_t* dst = out;
    while (out_len <= (n - 8)) {
        out_len += varint_codec.decodeBlock(src, dst + out_len);
    }

    // decodeBlock can overshoot, so we decode the last blocks in a local buffer
    while (out_len < n) {
        uint32_t buf[8];
        size_t read = varint_codec.decodeBlock(src, buf);
        size_t needed = std::min(read, n - out_len);
        memcpy(dst + out_len, buf, needed * 4);
        out_len += needed;
    }
    assert(out_len == n);
    return src;
}

}  // namespace pisa
