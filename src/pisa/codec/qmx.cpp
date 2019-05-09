#include "QMX/qmx.hpp"

#include "codec/block_codecs.hpp"
#include "codec/qmx.hpp"

namespace pisa {

using std::size_t;
using std::uint32_t;
using std::uint64_t;
using std::uint8_t;

void qmx_block::encode(uint32_t const *in,
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
    thread_local QMX::compress_integer_qmx_improved qmx_codec;
    thread_local std::vector<uint8_t> buf(2 * n * sizeof(uint32_t) + overflow);

    size_t out_len = qmx_codec.encode(buf.data(), buf.size(), in, n);
    TightVariableByte::encode_single(out_len, out);
    out.insert(out.end(), buf.data(), buf.data() + out_len);
}

auto qmx_block::decode(uint8_t const *in, uint32_t *out, uint32_t sum_of_values, size_t n)
    -> std::uint8_t const *
{
    static QMX::compress_integer_qmx_improved qmx_codec; // decodeBlock is thread-safe
    assert(n <= block_size);
    if (PISA_UNLIKELY(n < block_size)) {
        return interpolative_block::decode(in, out, sum_of_values, n);
    }
    uint32_t enc_len = 0;
    in = TightVariableByte::decode(in, &enc_len, 1);
    std::vector<uint32_t> buf(2 * n + overflow);
    qmx_codec.decode(buf.data(), n, in, enc_len);
    for (size_t i = 0; i < n; ++i) {
        *out = buf[i];
        ++out;
    }
    return in + enc_len;
}

} // namespace pisa
