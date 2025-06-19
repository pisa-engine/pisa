#pragma once

#include <string_view>
#include <vector>

#include "codec/block_codec.hpp"

namespace pisa {

/**
 * Varint-G8IU coding.
 *
 * Alexander A. Stepanov, Anil R. Gangolli, Daniel E. Rose, Ryan J. Ernst, and Paramjit S. Oberoi.
 * 2011. SIMD-based decoding of posting lists. In Proceedings of the 20th ACM international
 * conference on Information and knowledge management (CIKM '11), Bettina Berendt, Arjen de Vries,
 * Wenfei Fan, Craig Macdonald, Iadh Ounis, and Ian Ruthven (Eds.). ACM, New York, NY, USA, 317-326.
 * DOI: https://doi.org/10.1145/2063576.2063627
 */
class VarintG8IUBlockCodec: public BlockCodec {
    static const uint64_t m_block_size = 128;

  public:
    constexpr static std::string_view name = "block_varintg8iu";

    virtual ~VarintG8IUBlockCodec() = default;

    void encode(
        uint32_t const* in, uint32_t sum_of_values, size_t n, std::vector<uint8_t>& out
    ) const override;
    uint8_t const*
    decode(uint8_t const* in, uint32_t* out, uint32_t sum_of_values, size_t n) const override;
    auto block_size() const noexcept -> std::size_t override { return m_block_size; }
    auto get_name() const noexcept -> std::string_view override { return name; }
};

}  // namespace pisa
