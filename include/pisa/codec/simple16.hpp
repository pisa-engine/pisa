#pragma once

#include "codec/block_codec.hpp"

namespace pisa {

/**
 * Simple16 coding.
 *
 * Jiangong Zhang, Xiaohui Long, and Torsten Suel. 2008. Performance of compressed inverted list
 * caching in search engines. In Proceedings of the 17th international conference on World Wide Web
 * (WWW '08). ACM, New York, NY, USA, 387-396. DOI: https://doi.org/10.1145/1367497.1367550
 */
class Simple16BlockCodec: public BlockCodec {
    static constexpr std::uint64_t m_block_size = 128;

  public:
    constexpr static std::string_view name = "block_simple16";

    virtual ~Simple16BlockCodec() = default;

    void encode(uint32_t const* in, uint32_t sum_of_values, size_t n, std::vector<uint8_t>& out)
        const override;
    uint8_t const*
    decode(uint8_t const* in, uint32_t* out, uint32_t sum_of_values, size_t n) const override;
    auto block_size() const noexcept -> std::size_t override { return m_block_size; }
    auto get_name() const noexcept -> std::string_view override { return name; }
};

}  // namespace pisa
