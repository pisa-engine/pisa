#include <vector>
#include <cstdint>

#include "codec/block_codecs.hpp"
#include "codec/varintgb-detail.hpp"
#include "codec/varintgb.hpp"

namespace pisa {

void varintgb_block::encode(uint32_t const *in,
                            uint32_t sum_of_values,
                            size_t n,
                            std::vector<uint8_t> &out)
{
    thread_local VarIntGB<false> varintgb_codec;
    assert(n <= block_size);
    if (n < block_size) {
        interpolative_block::encode(in, sum_of_values, n, out);
        return;
    }
    thread_local std::vector<uint8_t> buf(2 * n * sizeof(uint32_t));
    size_t out_len = varintgb_codec.encodeArray(in, n, buf.data());
    out.insert(out.end(), buf.data(), buf.data() + out_len);
}

auto varintgb_block::decode(uint8_t const *in, uint32_t *out, uint32_t sum_of_values, size_t n)
    -> uint8_t const *
{
    thread_local VarIntGB<false> varintgb_codec;
    assert(n <= block_size);
    if (PISA_UNLIKELY(n < block_size)) {
        return interpolative_block::decode(in, out, sum_of_values, n);
    }
    auto read = varintgb_codec.decodeArray(in, n, out);
    return read + in;
}

} // namespace pisa
