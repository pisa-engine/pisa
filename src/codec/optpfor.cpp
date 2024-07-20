#include <cassert>

#include "codec/block_codecs.hpp"
#include "codec/optpfor.hpp"

namespace pisa {

uint32_t OptPForBlockCodec::Codec::findBestB(const uint32_t* in, uint32_t len) {
    // trick to force the choice of b from a parameter
    if (force_b != nullptr) {
        return *force_b;
    }

    // this is mostly a cut&paste from FastPFor, but we stop the
    // optimization early as the b to test becomes larger than maxb
    uint32_t b = 0;
    uint32_t bsize = std::numeric_limits<uint32_t>::max();
    const uint32_t mb = FastPForLib::maxbits(in, in + len);
    uint32_t i = 0;
    while (mb > 28 + possLogs[i]) {
        ++i;  // some schemes such as Simple16 don't code numbers greater than 28
    }

    for (; i < possLogs.size(); i++) {
        if (possLogs[i] > mb && possLogs[i] >= mb) {
            break;
        }
        const uint32_t csize = tryB(possLogs[i], in, len);

        if (csize <= bsize) {
            b = possLogs[i];
            bsize = csize;
        }
    }
    return b;
}

void OptPForBlockCodec::encode(
    uint32_t const* in, uint32_t sum_of_values, size_t n, std::vector<uint8_t>& out
) const {
    thread_local Codec optpfor_codec;
    thread_local std::array<std::uint8_t, 2 * 4 * m_block_size> buf{};
    assert(n <= m_block_size);

    if (n < m_block_size) {
        interpolative_block::encode(in, sum_of_values, n, out);
        return;
    }

    size_t out_len = buf.size();

    optpfor_codec.force_b = nullptr;
    optpfor_codec.encodeBlock(in, reinterpret_cast<uint32_t*>(buf.data()), out_len);
    out_len *= 4;
    out.insert(out.end(), buf.data(), buf.data() + out_len);
}

uint8_t const*
OptPForBlockCodec::decode(uint8_t const* in, uint32_t* out, uint32_t sum_of_values, size_t n) const {
    thread_local Codec optpfor_codec;  // pfor decoding is *not* thread-safe
    assert(n <= m_block_size);

    if PISA_UNLIKELY (n < m_block_size) {
        return interpolative_block::decode(in, out, sum_of_values, n);
    }

    size_t out_len = m_block_size;
    uint8_t const* ret;

    ret = reinterpret_cast<uint8_t const*>(
        optpfor_codec.decodeBlock(reinterpret_cast<uint32_t const*>(in), out, out_len)
    );
    assert(out_len == n);
    return ret;
}

}  // namespace pisa
