#pragma once

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <type_traits>
#include <utility>

#include <fmt/format.h>
#include <gsl/gsl_assert>
#include <gsl/span>
#include <tl/optional.hpp>

#include "codec/block_codecs.hpp"
#include "util/likely.hpp"
#include "v1/bit_cast.hpp"
#include "v1/cursor_traits.hpp"
#include "v1/encoding_traits.hpp"
#include "v1/types.hpp"
#include "v1/unaligned_span.hpp"

namespace pisa::v1 {

/// Uncompressed example of implementation of a single value cursor.
template <typename Codec, bool DeltaEncoded>
struct BlockedCursor {
    using value_type = std::uint32_t;

    /// Creates a cursor from the encoded bytes.
    explicit constexpr BlockedCursor(gsl::span<std::byte const> encoded_blocks,
                                     UnalignedSpan<value_type const> block_endpoints,
                                     UnalignedSpan<value_type const> block_last_values,
                                     std::uint32_t length,
                                     std::uint32_t num_blocks)
        : m_encoded_blocks(encoded_blocks),
          m_block_endpoints(block_endpoints),
          m_block_last_values(block_last_values),
          m_length(length),
          m_num_blocks(num_blocks),
          m_current_block(
              {.number = 0,
               .offset = 0,
               .length = std::min(length, static_cast<std::uint32_t>(Codec::block_size)),
               .last_value = m_block_last_values[0]})
    {
        static_assert(DeltaEncoded,
                      "Cannot initialize block_last_values for not delta-encoded list");
        m_decoded_block.resize(Codec::block_size);
        reset();
    }

    /// Creates a cursor from the encoded bytes.
    explicit constexpr BlockedCursor(gsl::span<std::byte const> encoded_blocks,
                                     UnalignedSpan<value_type const> block_endpoints,
                                     std::uint32_t length,
                                     std::uint32_t num_blocks)
        : m_encoded_blocks(encoded_blocks),
          m_block_endpoints(block_endpoints),
          m_length(length),
          m_num_blocks(num_blocks),
          m_current_block(
              {.number = 0,
               .offset = 0,
               .length = std::min(length, static_cast<std::uint32_t>(Codec::block_size)),
               .last_value = 0})
    {
        static_assert(not DeltaEncoded, "Must initialize block_last_values for delta-encoded list");
        m_decoded_block.resize(Codec::block_size);
        reset();
    }

    constexpr BlockedCursor(BlockedCursor const&) = default;
    constexpr BlockedCursor(BlockedCursor&&) noexcept = default;
    constexpr BlockedCursor& operator=(BlockedCursor const&) = default;
    constexpr BlockedCursor& operator=(BlockedCursor&&) noexcept = default;
    ~BlockedCursor() = default;

    void reset() { decode_and_update_block(0); }

    /// Dereferences the current value.
    [[nodiscard]] constexpr auto operator*() const -> value_type { return m_current_value; }

    /// Alias for `operator*()`.
    [[nodiscard]] constexpr auto value() const noexcept -> value_type { return *(*this); }

    /// Advances the cursor to the next position.
    constexpr void advance()
    {
        m_current_block.offset += 1;
        if (PISA_UNLIKELY(m_current_block.offset == m_current_block.length)) {
            if (m_current_block.number + 1 == m_num_blocks) {
                m_current_value = sentinel();
                return;
            }
            decode_and_update_block(m_current_block.number + 1);
        } else {
            if constexpr (DeltaEncoded) {
                m_current_value += m_decoded_block[m_current_block.offset] + 1U;
            } else {
                m_current_value = m_decoded_block[m_current_block.offset] + 1U;
            }
        }
    }

    /// Moves the cursor to the position `pos`.
    constexpr void advance_to_position(std::uint32_t pos)
    {
        Expects(pos >= position());
        auto block = pos / Codec::block_size;
        if (PISA_UNLIKELY(block != m_current_block.number)) {
            decode_and_update_block(block);
        }
        while (position() < pos) {
            if constexpr (DeltaEncoded) {
                m_current_value += m_decoded_block[++m_current_block.offset] + 1U;
            } else {
                m_current_value = m_decoded_block[++m_current_block.offset] + 1U;
            }
        }
    }

    /// Moves the cursor to the next value equal or greater than `value`.
    constexpr void advance_to_geq(value_type value)
    {
        // static_assert(DeltaEncoded, "Cannot call advance_to_geq on a not delta-encoded list");
        // TODO(michal): This should be `static_assert` like above. But currently,
        //               it would not compile. What needs to be done is separating document
        //               and payload readers for the index runner.
        assert(DeltaEncoded);
        if (PISA_UNLIKELY(value > m_current_block.last_value)) {
            if (value > m_block_last_values.back()) {
                m_current_value = sentinel();
                return;
            }
            auto block = m_current_block.number + 1U;
            while (m_block_last_values[block] < value) {
                ++block;
            }
            decode_and_update_block(block);
        }

        while (m_current_value < value) {
            m_current_value += m_decoded_block[++m_current_block.offset] + 1U;
            Ensures(m_current_block.offset < m_current_block.length);
        }
    }

    ///// Returns `true` if there is no elements left.
    [[nodiscard]] constexpr auto empty() const noexcept -> bool { return position() == m_length; }

    /// Returns the current position.
    [[nodiscard]] constexpr auto position() const noexcept -> std::size_t
    {
        return m_current_block.number * Codec::block_size + m_current_block.offset;
    }

    ///// Returns the number of elements in the list.
    [[nodiscard]] constexpr auto size() const -> std::size_t { return m_length; }

    /// The sentinel value, such that `value() != nullopt` is equivalent to `*(*this) < sentinel()`.
    [[nodiscard]] constexpr auto sentinel() const -> value_type
    {
        return std::numeric_limits<value_type>::max();
    }

   private:
    struct Block {
        std::uint32_t number = 0;
        std::uint32_t offset = 0;
        std::uint32_t length = 0;
        value_type last_value = 0;
    };

    void decode_and_update_block(std::uint32_t block)
    {
        constexpr auto block_size = Codec::block_size;
        auto endpoint = block > 0U ? m_block_endpoints[block - 1] : static_cast<value_type>(0U);
        std::uint8_t const* block_data =
            std::next(reinterpret_cast<std::uint8_t const*>(m_encoded_blocks.data()), endpoint);
        m_current_block.length =
            ((block + 1) * block_size <= size()) ? block_size : (size() % block_size);

        if constexpr (DeltaEncoded) {
            std::uint32_t first_value = block > 0U ? m_block_last_values[block - 1] + 1U : 0U;
            m_current_block.last_value = m_block_last_values[block];
            Codec::decode(block_data,
                          m_decoded_block.data(),
                          m_current_block.last_value - first_value - (m_current_block.length - 1),
                          m_current_block.length);
            m_decoded_block[0] += first_value;
        } else {
            Codec::decode(block_data,
                          m_decoded_block.data(),
                          std::numeric_limits<std::uint32_t>::max(),
                          m_current_block.length);
            m_decoded_block[0] += 1;
        }

        m_current_block.number = block;
        m_current_block.offset = 0U;
        m_current_value = m_decoded_block[0];
    }

    gsl::span<std::byte const> m_encoded_blocks;
    UnalignedSpan<const value_type> m_block_endpoints;
    UnalignedSpan<const value_type> m_block_last_values{};
    std::vector<value_type> m_decoded_block;

    std::uint32_t m_length;
    std::uint32_t m_num_blocks;
    Block m_current_block{};
    value_type m_current_value{};
};

template <bool DeltaEncoded>
constexpr auto block_encoding_type() -> std::uint32_t
{
    if constexpr (DeltaEncoded) {
        return EncodingId::BlockDelta;
    } else {
        return EncodingId::Block;
    }
}

template <typename Codec, bool DeltaEncoded>
struct BlockedReader {
    using value_type = std::uint32_t;

    [[nodiscard]] auto read(gsl::span<const std::byte> bytes) const
        -> BlockedCursor<Codec, DeltaEncoded>
    {
        std::uint32_t length;
        auto begin = reinterpret_cast<uint8_t const*>(bytes.data());
        auto after_length_ptr = pisa::TightVariableByte::decode(begin, &length, 1);
        auto length_byte_size = std::distance(begin, after_length_ptr);
        auto num_blocks = ceil_div(length, Codec::block_size);
        UnalignedSpan<value_type const> block_last_values;
        if constexpr (DeltaEncoded) {
            block_last_values = UnalignedSpan<value_type const>(
                bytes.subspan(length_byte_size, num_blocks * sizeof(value_type)));
        }
        UnalignedSpan<value_type const> block_endpoints(
            bytes.subspan(length_byte_size + block_last_values.byte_size(),
                          (num_blocks - 1) * sizeof(value_type)));
        auto encoded_blocks = bytes.subspan(length_byte_size + block_last_values.byte_size()
                                            + block_endpoints.byte_size());
        if constexpr (DeltaEncoded) {
            return BlockedCursor<Codec, DeltaEncoded>(
                encoded_blocks, block_endpoints, block_last_values, length, num_blocks);
        } else {
            return BlockedCursor<Codec, DeltaEncoded>(
                encoded_blocks, block_endpoints, length, num_blocks);
        }
    }

    constexpr static auto encoding() -> std::uint32_t
    {
        return block_encoding_type<DeltaEncoded>()
               | encoding_traits<Codec>::encoding_tag::encoding();
    }
};

template <typename Codec, bool DeltaEncoded>
struct BlockedWriter {
    using value_type = std::uint32_t;

    constexpr static auto encoding() -> std::uint32_t
    {
        return block_encoding_type<DeltaEncoded>()
               | encoding_traits<Codec>::encoding_tag::encoding();
    }

    void push(value_type const& posting)
    {
        if constexpr (DeltaEncoded) {
            if (posting < m_last_value) {
                throw std::runtime_error(
                    fmt::format("Delta-encoded sequences must be monotonic, but {} < {}",
                                posting,
                                m_last_value));
            }
        }
        m_postings.push_back(posting);
        m_last_value = posting;
    }

    template <typename CharT>
    [[nodiscard]] auto write(std::basic_ostream<CharT>& os) const -> std::size_t
    {
        std::vector<std::uint8_t> buffer;
        std::uint32_t length = m_postings.size();
        TightVariableByte::encode_single(length, buffer);
        auto block_size = Codec::block_size;
        auto num_blocks = ceil_div(length, block_size);
        auto begin_block_maxs = buffer.size();
        auto begin_block_endpoints = [&]() {
            if constexpr (DeltaEncoded) {
                return begin_block_maxs + 4U * num_blocks;
            } else {
                return begin_block_maxs;
            }
        }();
        auto begin_blocks = begin_block_endpoints + 4U * (num_blocks - 1);
        buffer.resize(begin_blocks);

        auto iter = m_postings.begin();
        std::vector<value_type> block_buffer(block_size);
        std::uint32_t last_value(-1);
        std::uint32_t block_base = 0;
        for (auto block = 0; block < num_blocks; ++block) {
            auto current_block_size =
                ((block + 1) * block_size <= length) ? block_size : (length % block_size);

            std::for_each(block_buffer.begin(),
                          std::next(block_buffer.begin(), current_block_size),
                          [&](auto&& elem) {
                              if constexpr (DeltaEncoded) {
                                  auto value = *iter++;
                                  elem = value - (last_value + 1);
                                  last_value = value;
                              } else {
                                  elem = *iter++ - 1;
                              }
                          });

            if constexpr (DeltaEncoded) {
                std::memcpy(
                    &buffer[begin_block_maxs + 4U * block], &last_value, sizeof(last_value));
                auto size = buffer.size();
                Codec::encode(block_buffer.data(),
                              last_value - block_base - (current_block_size - 1),
                              current_block_size,
                              buffer);
            } else {
                Codec::encode(block_buffer.data(), std::uint32_t(-1), current_block_size, buffer);
            }
            if (block != num_blocks - 1) {
                std::size_t endpoint = buffer.size() - begin_blocks;
                std::memcpy(
                    &buffer[begin_block_endpoints + 4U * block], &endpoint, sizeof(last_value));
            }
            block_base = last_value + 1;
        }
        os.write(reinterpret_cast<CharT const*>(buffer.data()), buffer.size());
        return buffer.size();
    }

    void reset()
    {
        m_postings.clear();
        m_last_value = 0;
    }

   private:
    std::vector<value_type> m_postings{};
    value_type m_last_value = 0U;
};

template <typename T, bool DeltaEncoded>
struct CursorTraits<BlockedCursor<T, DeltaEncoded>> {
    using Writer = BlockedWriter<T, DeltaEncoded>;
    using Reader = BlockedReader<T, DeltaEncoded>;
};

} // namespace pisa::v1
