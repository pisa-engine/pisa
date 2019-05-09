#include <cassert>
#include <cstdint>
#include <vector>

#include "streamvbyte/include/streamvbyte.h"

#include "codec/streamvbyte.hpp"

namespace pisa {

using std::uint32_t;
using std::uint64_t;
using std::uint8_t;

void streamvbyte_block::encode(uint32_t const *in,
                               uint32_t /* sum_of_values */,
                               size_t n,
                               std::vector<uint8_t> &out)
{

    assert(n <= block_size);
    uint32_t *src = const_cast<uint32_t *>(in);
    thread_local std::vector<uint8_t> buf(streamvbyte_max_compressedbytes(block_size));
    size_t out_len = streamvbyte_encode(src, n, buf.data());
    out.insert(out.end(), buf.data(), buf.data() + out_len);
}

auto streamvbyte_block::decode(uint8_t const *in,
                               uint32_t *out,
                               uint32_t /* sum_of_values */,
                               size_t n) -> uint8_t const *
{
    assert(n <= block_size);
    auto read = streamvbyte_decode(in, out, n);
    return in + read;
}
} // namespace pisa
