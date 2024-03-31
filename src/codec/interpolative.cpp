#include <cassert>
#include <limits>

#include "codec/block_codecs.hpp"
#include "codec/interpolative.hpp"

namespace pisa {

void InterpolativeBlockCodec::encode(
    uint32_t const* in, uint32_t sum_of_values, size_t n, std::vector<uint8_t>& out
) const {
    assert(n <= m_block_size);
    thread_local std::array<std::uint32_t, m_block_size> inbuf{};
    thread_local std::vector<uint32_t> outbuf;  // TODO: Can we use array? How long does it need
                                                // to be?
    inbuf[0] = *in;
    for (size_t i = 1; i < n; ++i) {
        inbuf[i] = inbuf[i - 1] + in[i];
    }

    if (sum_of_values == uint32_t(-1)) {
        sum_of_values = inbuf[n - 1];
        TightVariableByte::encode_single(sum_of_values, out);
    }

    bit_writer bw(outbuf);
    bw.write_interpolative(inbuf.data(), n - 1, 0, sum_of_values);
    auto const* bufptr = reinterpret_cast<uint8_t const*>(outbuf.data());
    out.insert(out.end(), bufptr, bufptr + ceil_div(bw.size(), 8));
}

uint8_t const* InterpolativeBlockCodec::decode(
    uint8_t const* in, uint32_t* out, uint32_t sum_of_values, size_t n
) const {
    assert(n <= m_block_size);
    if (sum_of_values == std::numeric_limits<std::uint32_t>::max()) {
        in = TightVariableByte::decode(in, &sum_of_values, 1);
    }

    out[n - 1] = sum_of_values;
    size_t read_interpolative = 0;
    if (n > 1) {
        bit_reader br(in);
        br.read_interpolative(out, n - 1, 0, sum_of_values);
        for (size_t i = n - 1; i > 0; --i) {
            out[i] -= out[i - 1];
        }
        read_interpolative = ceil_div(br.position(), 8);
    }

    return in + read_interpolative;
}

}  // namespace pisa
