#include "codec/simple8b.hpp"

#include "FastPFor/headers/simple8b.h"

namespace pisa {

const uint64_t simple8b_block::block_size = 128;

void simple8b_block::encode(
    uint32_t const* in, uint32_t /* sum_of_values */, size_t n, std::vector<uint8_t>& out)
{
    assert(n <= block_size);
    thread_local FastPForLib::Simple8b<false> codec;
    thread_local std::vector<uint8_t> buf(2 * 8 * block_size);
    size_t out_len = buf.size();
    codec.encodeArray(in, n, reinterpret_cast<uint32_t*>(buf.data()), out_len);
    out_len *= 4;
    out.insert(out.end(), buf.data(), buf.data() + out_len);
}

uint8_t const*
simple8b_block::decode(uint8_t const* in, uint32_t* out, uint32_t /* sum_of_values */, size_t n)
{
    assert(n <= block_size);
    FastPForLib::Simple8b<false> codec;
    return reinterpret_cast<uint8_t const*>(
        codec.decodeArray(reinterpret_cast<uint32_t const*>(in), 8 * n, out, n));
}

}  // namespace pisa
