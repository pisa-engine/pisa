#include <cstdint>
#include <vector>

#include "MaskedVByte/include/varintdecode.h"
#include "MaskedVByte/include/varintencode.h"

#include "codec/block_codecs.hpp"
#include "codec/maskedvbyte.hpp"
#include "util/util.hpp"

namespace pisa {

using std::uint32_t;
using std::uint64_t;
using std::uint8_t;

void maskedvbyte_block::encode(uint32_t const *in,
                               uint32_t sum_of_values,
                               size_t n,
                               std::vector<uint8_t> &out)
{

    assert(n <= block_size);
    uint32_t *src = const_cast<uint32_t *>(in);
    if (n < block_size) {
        interpolative_block::encode(src, sum_of_values, n, out);
        return;
    }
    thread_local std::vector<uint8_t> buf(2 * n * sizeof(uint32_t));
    size_t out_len = vbyte_encode(src, n, buf.data());
    out.insert(out.end(), buf.data(), buf.data() + out_len);
}

auto maskedvbyte_block::decode(uint8_t const *in, uint32_t *out, uint32_t sum_of_values, size_t n)
    -> uint8_t const *
{
    assert(n <= block_size);
    if (PISA_UNLIKELY(n < block_size)) {
        return interpolative_block::decode(in, out, sum_of_values, n);
    }
    auto read = masked_vbyte_decode(in, out, n);
    return in + read;
}

} // namespace pisa
