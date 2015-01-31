#pragma once

namespace ds2i {

    // note: n can be 0
    void write_gamma(succinct::bit_vector_builder& bvb, uint64_t n)
    {
        uint64_t nn = n + 1;
        uint64_t l = succinct::broadword::msb(nn);
        uint64_t hb = uint64_t(1) << l;
        bvb.append_bits(hb, l + 1);
        bvb.append_bits(nn ^ hb, l);
    }

    void write_gamma_nonzero(succinct::bit_vector_builder& bvb, uint64_t n)
    {
        assert(n > 0);
        write_gamma(bvb, n - 1);
    }

    uint64_t read_gamma(succinct::bit_vector::enumerator& it)
    {
        uint64_t l = it.skip_zeros();
        return (it.take(l) | (uint64_t(1) << l)) - 1;
    }

    uint64_t read_gamma_nonzero(succinct::bit_vector::enumerator& it)
    {
        return read_gamma(it) + 1;
    }

    void write_delta(succinct::bit_vector_builder& bvb, uint64_t n)
    {
        uint64_t nn = n + 1;
        uint64_t l = succinct::broadword::msb(nn);
        uint64_t hb = uint64_t(1) << l;
        write_gamma(bvb, l);
        bvb.append_bits(nn ^ hb, l);
    }

    uint64_t read_delta(succinct::bit_vector::enumerator& it)
    {
        uint64_t l = read_gamma(it);
        return (it.take(l) | (uint64_t(1) << l)) - 1;
    }
}
