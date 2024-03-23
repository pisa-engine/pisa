#pragma once

#include <cstdint>
#include <vector>

#include "codec/block_codecs.hpp"
#include "util/util.hpp"

extern "C" {
#include "simdcomp/include/simdbitpacking.h"
}

namespace pisa {

class BlockCodec {
  public:
    virtual void encode(
        std::uint32_t const* in, std::uint32_t sum_of_values, std::size_t n, std::vector<uint8_t>& out
    ) const = 0;

    virtual std::uint8_t const* decode(
        std::uint8_t const* in, std::uint32_t* out, std::uint32_t sum_of_values, std::size_t n
    ) const = 0;

    [[nodiscard]] virtual auto block_size() const noexcept -> std::size_t = 0;
};

class SimdBpBlockCodec: public BlockCodec {
    static constexpr std::uint64_t m_block_size = 128;

    void encode(uint32_t const* in, uint32_t sum_of_values, size_t n, std::vector<uint8_t>& out) const {
        assert(n <= m_block_size);
        auto* src = const_cast<uint32_t*>(in);
        if (n < m_block_size) {
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

    uint8_t const* decode(uint8_t const* in, uint32_t* out, uint32_t sum_of_values, size_t n) const {
        assert(n <= m_block_size);
        if PISA_UNLIKELY (n < m_block_size) {
            return interpolative_block::decode(in, out, sum_of_values, n);
        }
        uint32_t b = *in++;
        simdunpack((const __m128i*)in, out, b);
        return in + b * sizeof(__m128i);
    }

    auto block_size() const noexcept -> std::size_t { return m_block_size; }
};

class Simple16BlockCodec: public BlockCodec {
    static constexpr std::uint64_t m_block_size = 128;

    void
    encode(uint32_t const* in, uint32_t /* sum_of_values */, size_t n, std::vector<uint8_t>& out) const {
        assert(n <= m_block_size);
        thread_local FastPForLib::Simple16<false> codec;
        thread_local std::array<std::uint8_t, 2 * 8 * m_block_size> buf{};
        size_t out_len = buf.size();
        codec.encodeArray(in, n, reinterpret_cast<uint32_t*>(buf.data()), out_len);
        out_len *= 4;
        out.insert(out.end(), buf.data(), buf.data() + out_len);
    }

    uint8_t const* decode(uint8_t const* in, uint32_t* out, uint32_t sum_of_values, size_t n) const {
        assert(n <= m_block_size);
        FastPForLib::Simple16<false> codec;
        std::array<std::uint32_t, 2 * m_block_size> buf{};

        auto const* ret = reinterpret_cast<uint8_t const*>(
            codec.decodeArray(reinterpret_cast<uint32_t const*>(in), 8 * n, buf.data(), n)
        );

        std::copy(buf.begin(), std::next(buf.begin(), n), out);
        return ret;
    }

    auto block_size() const noexcept -> std::size_t { return m_block_size; }
};

};  // namespace pisa
