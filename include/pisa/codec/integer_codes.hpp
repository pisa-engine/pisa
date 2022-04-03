#pragma once

#include "bit_vector.hpp"
#include "bit_vector_builder.hpp"

namespace pisa {

// note: n can be 0
inline void write_gamma(bit_vector_builder& bvb, uint64_t n)
{
    uint64_t nn = n + 1;
    uint64_t l = broadword::msb(nn);
    uint64_t hb = uint64_t(1) << l;
    bvb.append_bits(hb, l + 1);
    bvb.append_bits(nn ^ hb, l);
}

inline void write_gamma_nonzero(bit_vector_builder& bvb, uint64_t n)
{
    assert(n > 0);
    write_gamma(bvb, n - 1);
}

inline uint64_t read_gamma(bit_vector::enumerator& it)
{
    uint64_t l = it.skip_zeros();
    assert(l < 64);
    return (it.take(l) | (uint64_t(1) << l)) - 1;
}

inline uint64_t read_gamma_nonzero(bit_vector::enumerator& it)
{
    return read_gamma(it) + 1;
}

inline void write_delta(bit_vector_builder& bvb, uint64_t n)
{
    uint64_t nn = n + 1;
    uint64_t l = broadword::msb(nn);
    uint64_t hb = uint64_t(1) << l;
    write_gamma(bvb, l);
    bvb.append_bits(nn ^ hb, l);
}

inline uint64_t read_delta(bit_vector::enumerator& it)
{
    uint64_t l = read_gamma(it);
    return (it.take(l) | (uint64_t(1) << l)) - 1;
}
}  // namespace pisa
