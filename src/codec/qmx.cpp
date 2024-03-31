#include "codec/qmx.hpp"

namespace pisa {

void QmxBlockCodec::encode(
    uint32_t const* in, uint32_t sum_of_values, size_t n, std::vector<uint8_t>& out
) const {
    assert(n <= m_block_size);
    auto* src = const_cast<uint32_t*>(in);
    if (n < m_block_size) {
        interpolative_block::encode(src, sum_of_values, n, out);
        return;
    }
    thread_local QMX::compress_integer_qmx_improved qmx_codec;
    thread_local std::vector<std::uint8_t> buf(2 * n * sizeof(uint32_t) + m_overflow);

    size_t out_len = qmx_codec.encode(buf.data(), buf.size(), in, n);
    TightVariableByte::encode_single(out_len, out);
    out.insert(out.end(), buf.data(), buf.data() + out_len);
}

uint8_t const*
QmxBlockCodec::decode(uint8_t const* in, uint32_t* out, uint32_t sum_of_values, size_t n) const {
    static QMX::compress_integer_qmx_improved qmx_codec;  // decodeBlock is thread-safe
    assert(n <= m_block_size);
    if PISA_UNLIKELY (n < m_block_size) {
        return interpolative_block::decode(in, out, sum_of_values, n);
    }
    uint32_t enc_len = 0;
    in = TightVariableByte::decode(in, &enc_len, 1);
    std::vector<uint32_t> buf(2 * n + m_overflow);
    qmx_codec.decode(buf.data(), n, in, enc_len);
    for (size_t i = 0; i < n; ++i) {
        *out = buf[i];
        ++out;
    }
    return in + enc_len;
}

}  // namespace pisa
