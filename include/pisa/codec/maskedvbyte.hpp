#pragma once

#include <vector>

#include "codec/block_codec.hpp"

namespace pisa {

/**
 * Masked VByte coding.
 *
 * Jeff Plaisance, Nathan Kurz, Daniel Lemire, Vectorized VByte Decoding, International Symposium on
 * Web Algorithms 2015, 2015.
 */
class MaskedVByteBlockCodec: public BlockCodec {
    static constexpr std::uint64_t m_block_size = 128;
    static constexpr std::uint64_t m_overflow = 512;

  public:
    constexpr static std::string_view name = "block_maskedvbyte";

    virtual ~MaskedVByteBlockCodec() = default;

    void encode(uint32_t const* in, uint32_t sum_of_values, size_t n, std::vector<uint8_t>& out)
        const override;
    uint8_t const*
    decode(uint8_t const* in, uint32_t* out, uint32_t sum_of_values, size_t n) const override;
    auto block_size() const noexcept -> std::size_t override { return m_block_size; }
    auto get_name() const noexcept -> std::string_view override { return name; }
};

}  // namespace pisa
