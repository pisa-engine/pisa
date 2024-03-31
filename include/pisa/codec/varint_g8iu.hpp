#pragma once

#include <string_view>
#include <vector>

#include "codec/block_codec.hpp"

namespace pisa {

class VarintG8IUBlockCodec: public BlockCodec {
    static const uint64_t m_block_size = 128;

  public:
    constexpr static std::string_view name = "block_varintg8iu";

    void encode(uint32_t const* in, uint32_t sum_of_values, size_t n, std::vector<uint8_t>& out) const;
    uint8_t const* decode(uint8_t const* in, uint32_t* out, uint32_t sum_of_values, size_t n) const;
    auto block_size() const noexcept -> std::size_t { return m_block_size; }
};

}  // namespace pisa
