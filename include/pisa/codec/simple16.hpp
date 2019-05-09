#pragma once

#include <cstdint>
#include <vector>

namespace pisa {

struct simple16_block {
    static const std::uint64_t block_size = 128;
    static void encode(std::uint32_t const *in,
                       std::uint32_t sum_of_values,
                       std::size_t n,
                       std::vector<uint8_t> &out);

    static auto decode(std::uint8_t const *in,
                       std::uint32_t *out,
                       std::uint32_t sum_of_values,
                       std::size_t n) -> std::uint8_t const *;
};

} // namespace pisa
