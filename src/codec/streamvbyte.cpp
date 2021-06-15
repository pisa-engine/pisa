#include "codec/streamvbyte.hpp"

#include <cassert>
#include <vector>

#include "streamvbyte/include/streamvbyte.h"

namespace pisa {

const uint64_t streamvbyte_block::block_size = 128;

void streamvbyte_block::encode(
    uint32_t const* in, uint32_t /* sum_of_values */, size_t n, std::vector<uint8_t>& out)
{
    assert(n <= block_size);
    auto* src = const_cast<uint32_t*>(in);
    thread_local std::vector<uint8_t> buf(streamvbyte_max_compressedbytes(block_size));
    size_t out_len = streamvbyte_encode(src, n, buf.data());
    out.insert(out.end(), buf.data(), buf.data() + out_len);
}

uint8_t const*
streamvbyte_block::decode(uint8_t const* in, uint32_t* out, uint32_t /* sum_of_values */, size_t n)
{
    assert(n <= block_size);
    auto read = streamvbyte_decode(in, out, n);
    return in + read;
}

}  // namespace pisa
