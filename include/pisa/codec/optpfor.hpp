#pragma once

#include <vector>

#include "FastPFor/headers/optpfor.h"

#include "codec/block_codec.hpp"

namespace pisa {

class OptPForBlockCodec: public BlockCodec {
    struct Codec: FastPForLib::OPTPFor<4, FastPForLib::Simple16<false>> {
        uint8_t const* force_b{nullptr};

        uint32_t findBestB(const uint32_t* in, uint32_t len) {
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
    };

    static const uint64_t m_block_size = Codec::BlockSize;

  public:
    constexpr static std::string_view name = "block_optpfor";

    void encode(uint32_t const* in, uint32_t sum_of_values, size_t n, std::vector<uint8_t>& out)
        const override;
    uint8_t const*
    decode(uint8_t const* in, uint32_t* out, uint32_t sum_of_values, size_t n) const override;
    auto block_size() const noexcept -> std::size_t override { return m_block_size; }
    auto get_name() const noexcept -> std::string_view override { return name; }
};

}  // namespace pisa
