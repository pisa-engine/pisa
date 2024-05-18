#pragma once

#include "codec/block_codec.hpp"

namespace pisa {

/**
 * Simple8b coding.
 *
 * Vo Ngoc Anh, Alistair Moffat: Index compression using 64-bit words. Softw., Pract. Exper. 40(2):
 * 131-147 (2010)
 */
class Simple8bBlockCodec: public BlockCodec {
    static constexpr std::uint64_t m_block_size = 128;

  public:
    constexpr static std::string_view name = "block_simple8b";

    void encode(uint32_t const* in, uint32_t sum_of_values, size_t n, std::vector<uint8_t>& out)
        const override;
    uint8_t const*
    decode(uint8_t const* in, uint32_t* out, uint32_t sum_of_values, size_t n) const override;
    auto block_size() const noexcept -> std::size_t override { return m_block_size; }
    auto get_name() const noexcept -> std::string_view override { return name; }
};

}  // namespace pisa
