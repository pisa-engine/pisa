#pragma once

#include <cstdint>
#include <vector>

namespace pisa {

struct varintgb_block {
    static const uint64_t block_size = 128;
    static void encode(uint32_t const *in,
                       uint32_t sum_of_values,
                       size_t n,
                       std::vector<uint8_t> &out);
    static auto decode(uint8_t const *in, uint32_t *out, uint32_t sum_of_values, size_t n)
        -> uint8_t const *;
};

} // namespace pisa
