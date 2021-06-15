#pragma once

#include <cstdint>
#include <cstdio>
#include <vector>

namespace pisa {

struct qmx_block {
    static const uint64_t block_size;
    static const uint64_t overflow;

    static void encode(uint32_t const* in, uint32_t sum_of_values, size_t n, std::vector<uint8_t>& out);

    static uint8_t const* decode(uint8_t const* in, uint32_t* out, uint32_t sum_of_values, size_t n);
};

}  // namespace pisa
