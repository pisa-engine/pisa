#pragma once
#include "configuration.hpp"

namespace pisa {

float dequantize(uint64_t value)
{
    const float quant = 1.f / configuration::get().reference_size;
    return quant * (value + 1);
}

uint64_t quantize(float value)
{
    float quant = 1.f / configuration::get().reference_size;
    uint64_t pos = 1;
    while (value > quant * pos)
        pos++;
    return pos;
}

} // namespace pisa