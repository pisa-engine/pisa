#pragma once

#include "codec/block_codec.hpp"

namespace pisa {

/**
 * Quantities, Multipliers, and eXtractor (QMX) coding.
 *
 * Andrew Trotman. 2014. Compression, SIMD, and Postings Lists. In Proceedings of the 2014
 * Australasian Document Computing Symposium (ADCS '14), J. Shane Culpepper, Laurence Park, and
 * Guido Zuccon (Eds.). ACM, New York, NY, USA, Pages 50, 8 pages. DOI:
 * https://doi.org/10.1145/2682862.2682870
 */
class QmxBlockCodec: public BlockCodec {
    static constexpr std::uint64_t m_block_size = 128;
    static constexpr std::uint64_t m_overflow = 512;

  public:
    constexpr static std::string_view name = "block_qmx";

    void encode(uint32_t const* in, uint32_t sum_of_values, size_t n, std::vector<uint8_t>& out)
        const override;

    /**
     * NOTE: In order to be sure to avoid undefined behavior, `in` must be padded with 15 bytes
     * because of 16-byte SIMD reads that could be called with the address of the last byte.
     * This is NOT enforced by `encode`, because it would be very wasteful to add 15 bytes to each
     * block. Instead, the padding is added to the index, after all postings.
     */
    uint8_t const*
    decode(uint8_t const* in, uint32_t* out, uint32_t sum_of_values, size_t n) const override;
    auto block_size() const noexcept -> std::size_t override { return m_block_size; }
    auto get_name() const noexcept -> std::string_view override { return name; }
};

}  // namespace pisa
