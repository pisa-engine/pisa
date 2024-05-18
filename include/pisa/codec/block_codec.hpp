#pragma once

#include <cstdint>
#include <memory>
#include <string_view>
#include <vector>

namespace pisa {

/**
 * Block codecs encode and decode a list of integers. This is in opposition to a streaming codec,
 * which can encode and decode values one by one.
 */
class BlockCodec {
  public:
    /**
     * Encodes a list of `n` unsigned integers and appends them to the output buffer.
     */
    virtual void encode(
        std::uint32_t const* in, std::uint32_t sum_of_values, std::size_t n, std::vector<uint8_t>& out
    ) const = 0;

    /**
     * Decodes a list of `n` unsigned integers from a binary buffer and writes them to pre-allocated
     * memory.
     */
    virtual std::uint8_t const* decode(
        std::uint8_t const* in, std::uint32_t* out, std::uint32_t sum_of_values, std::size_t n
    ) const = 0;

    /**
     * Returns the block size of the encoding.
     *
     * Block codecs write blocks of fixed size, e.g., 128 integers. Thus, it is only possible to
     * encode at most `block_size()` elements with a single `encode` call.
     */
    [[nodiscard]] virtual auto block_size() const noexcept -> std::size_t = 0;

    /**
     * Returns the name of the codec.
     */
    [[nodiscard]] virtual auto get_name() const noexcept -> std::string_view = 0;
};

using BlockCodecPtr = std::shared_ptr<BlockCodec>;

};  // namespace pisa
