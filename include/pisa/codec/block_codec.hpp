#pragma once

#include <cstdint>
#include <vector>

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

};  // namespace pisa
