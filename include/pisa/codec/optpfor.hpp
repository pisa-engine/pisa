#pragma once

#include <vector>

#include "FastPFor/headers/optpfor.h"

#include "codec/block_codec.hpp"

namespace pisa {

/**
 * OptPForDelta coding.
 *
 * Hao Yan, Shuai Ding, and Torsten Suel. 2009. Inverted index compression and query processing with
 * optimized document ordering. In Proceedings of the 18th international conference on World wide
 * web (WWW '09). ACM, New York, NY, USA, 401-410. DOI: https://doi.org/10.1145/1526709.1526764
 */
class OptPForBlockCodec: public BlockCodec {
    struct Codec: FastPForLib::OPTPFor<4, FastPForLib::Simple16<false>> {
        uint8_t const* force_b = nullptr;
        uint32_t findBestB(const uint32_t* in, uint32_t len);
    };

    static const uint64_t m_block_size = Codec::BlockSize;

  public:
    constexpr static std::string_view name = "block_optpfor";

    virtual ~OptPForBlockCodec() = default;

    void encode(
        uint32_t const* in, uint32_t sum_of_values, size_t n, std::vector<uint8_t>& out
    ) const override;
    uint8_t const*
    decode(uint8_t const* in, uint32_t* out, uint32_t sum_of_values, size_t n) const override;
    auto block_size() const noexcept -> std::size_t override { return m_block_size; }
    auto get_name() const noexcept -> std::string_view override { return name; }
};

}  // namespace pisa
