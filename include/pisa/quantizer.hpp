#pragma once
#include "configuration.hpp"
namespace pisa {

uint64_t quantize(float value)
{
    float quant = 1.f / configuration::get().reference_size;
    uint64_t pos = 1;
    while (value > quant * pos)
        pos++;
    return pos;
}

} // pisa
