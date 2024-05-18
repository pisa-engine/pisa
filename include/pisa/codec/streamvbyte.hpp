#pragma once

#include <cassert>
#include <cstdint>
#include <string_view>
#include <vector>

#include "codec/block_codec.hpp"

namespace pisa {

// This is a constexpr version of the function in the streamvbyte library.
constexpr std::size_t streamvbyte_max_compressedbytes(std::uint32_t length) {
    // number of control bytes:
    size_t cb = (length + 3) / 4;
    // maximum number of control bytes:
    size_t db = (size_t)length * sizeof(uint32_t);
    return cb + db;
}

/**
 * StreamVByte coding.
 *
 * Daniel Lemire, Nathan Kurz, Christoph Rupp: Stream VByte: Faster byte-oriented integer
 * compression. Inf. Process. Lett. 130: 1-6 (2018). DOI: https://doi.org/10.1016/j.ipl.2017.09.011
 */
class StreamVByteBlockCodec: public BlockCodec {
    static constexpr std::uint64_t m_block_size = 128;
    static constexpr std::size_t m_max_compressed_bytes =
        pisa::streamvbyte_max_compressedbytes(m_block_size);

  public:
    constexpr static std::string_view name = "block_streamvbyte";

    void encode(uint32_t const* in, uint32_t sum_of_values, size_t n, std::vector<uint8_t>& out)
        const override;
    uint8_t const*
    decode(uint8_t const* in, uint32_t* out, uint32_t sum_of_values, size_t n) const override;
    auto block_size() const noexcept -> std::size_t override { return m_block_size; }
    auto get_name() const noexcept -> std::string_view override { return name; }
};

}  // namespace pisa
