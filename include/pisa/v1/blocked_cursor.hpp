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

#include "util/likely.hpp"
#include "v1/base_index.hpp"
#include "v1/bit_cast.hpp"
#include "v1/cursor_traits.hpp"
#include "v1/encoding_traits.hpp"
#include "v1/types.hpp"
#include "v1/unaligned_span.hpp"

namespace pisa::v1 {

/// Non-template base of blocked cursors.
struct BaseBlockedCursor {
    using value_type = std::uint32_t;
    using offset_type = std::uint32_t;
    using size_type = std::uint32_t;

    /// Creates a cursor from the encoded bytes.
    BaseBlockedCursor(gsl::span<std::byte const> encoded_blocks,
                      UnalignedSpan<value_type const> block_endpoints,
                      size_type length,
                      size_type num_blocks,
                      size_type block_length)
        : m_encoded_blocks(encoded_blocks),
          m_block_endpoints(block_endpoints),
          m_decoded_block(block_length),
          m_length(length),
          m_num_blocks(num_blocks),
          m_block_length(block_length),
          m_current_block({.number = 0,
                           .offset = 0,
                           .length = std::min(length, static_cast<size_type>(m_block_length))})
    {
    }

    BaseBlockedCursor(BaseBlockedCursor const&) = default;
    BaseBlockedCursor(BaseBlockedCursor&&) noexcept = default;
    BaseBlockedCursor& operator=(BaseBlockedCursor const&) = default;
    BaseBlockedCursor& operator=(BaseBlockedCursor&&) noexcept = default;
    ~BaseBlockedCursor() = default;

    [[nodiscard]] auto operator*() const -> value_type;
    [[nodiscard]] auto value() const noexcept -> value_type;
    [[nodiscard]] auto empty() const noexcept -> bool;
    [[nodiscard]] auto position() const noexcept -> std::size_t;
    [[nodiscard]] auto size() const -> std::size_t;
    [[nodiscard]] auto sentinel() const -> value_type;

   protected:
    struct Block {
        std::uint32_t number = 0;
        std::uint32_t offset = 0;
        std::uint32_t length = 0;
    };

    [[nodiscard]] auto block_offset(size_type block) const -> offset_type;
    [[nodiscard]] auto decoded_block() -> value_type*;
    [[nodiscard]] auto decoded_value(size_type n) -> value_type;
    [[nodiscard]] auto encoded_block(offset_type offset) -> uint8_t const*;
    [[nodiscard]] auto length() const -> size_type;
    [[nodiscard]] auto num_blocks() const -> size_type;
    [[nodiscard]] auto current_block() -> Block&;
    void update_current_value(value_type val);
    void increase_current_value(value_type val);

   private:
    gsl::span<std::byte const> m_encoded_blocks;
    UnalignedSpan<const offset_type> m_block_endpoints;
    std::vector<value_type> m_decoded_block{};
    size_type m_length;
    size_type m_num_blocks;
    size_type m_block_length;
    Block m_current_block;

    value_type m_current_value{};
};

template <typename Codec, bool DeltaEncoded>
struct GenericBlockedCursor : public BaseBlockedCursor {
    using BaseBlockedCursor::offset_type;
    using BaseBlockedCursor::size_type;
    using BaseBlockedCursor::value_type;

    GenericBlockedCursor(gsl::span<std::byte const> encoded_blocks,
                         UnalignedSpan<offset_type const> block_endpoints,
                         UnalignedSpan<value_type const> block_last_values,
                         std::uint32_t length,
                         std::uint32_t num_blocks)
        : BaseBlockedCursor(encoded_blocks, block_endpoints, length, num_blocks, Codec::block_size),
          m_block_last_values(block_last_values),
          m_current_block_last_value(m_block_last_values.empty() ? value_type{}
                                                                 : m_block_last_values[0])
    {
        reset();
    }

    void reset() { decode_and_update_block(0); }

    /// Advances the cursor to the next position.
    void advance()
    {
        auto& current_block = this->current_block();
        current_block.offset += 1;
        if (PISA_UNLIKELY(current_block.offset == current_block.length)) {
            if (current_block.number + 1 == num_blocks()) {
                update_current_value(sentinel());
                return;
            }
            decode_and_update_block(current_block.number + 1);
        } else {
            if constexpr (DeltaEncoded) {
                increase_current_value(decoded_value(current_block.offset));
            } else {
                update_current_value(decoded_value(current_block.offset));
            }
        }
    }

    /// Moves the cursor to the position `pos`.
    void advance_to_position(std::uint32_t pos)
    {
        Expects(pos >= position());
        auto& current_block = this->current_block();
        auto block = pos / Codec::block_size;
        if (PISA_UNLIKELY(block != current_block.number)) {
            decode_and_update_block(block);
        }
        while (position() < pos) {
            current_block.offset += 1;
            if constexpr (DeltaEncoded) {
                increase_current_value(decoded_value(current_block.offset));
            } else {
                update_current_value(decoded_value(current_block.offset));
            }
        }
    }

   protected:
    [[nodiscard]] auto& block_last_values() { return m_block_last_values; }
    [[nodiscard]] auto& current_block_last_value() { return m_current_block_last_value; }

    void decode_and_update_block(size_type block)
    {
        auto block_size = Codec::block_size;
        auto const* block_data = encoded_block(block_offset(block));
        auto& current_block = this->current_block();
        current_block.length =
            ((block + 1) * block_size <= size()) ? block_size : (size() % block_size);

        if constexpr (DeltaEncoded) {
            std::uint32_t first_value = block > 0U ? m_block_last_values[block - 1] + 1U : 0U;
            m_current_block_last_value = m_block_last_values[block];
            Codec::decode(block_data,
                          decoded_block(),
                          m_current_block_last_value - first_value - (current_block.length - 1),
                          current_block.length);
            decoded_block()[0] += first_value;
        } else {
            Codec::decode(block_data,
                          decoded_block(),
                          std::numeric_limits<std::uint32_t>::max(),
                          current_block.length);
            decoded_block()[0] += 1;
        }

        current_block.number = block;
        current_block.offset = 0U;
        update_current_value(decoded_block()[0]);
    }

   private:
    UnalignedSpan<const value_type> m_block_last_values{};
    value_type m_current_block_last_value{};
};

template <typename Codec>
struct DocumentBlockedCursor : public GenericBlockedCursor<Codec, true> {
    using offset_type = typename GenericBlockedCursor<Codec, true>::offset_type;
    using size_type = typename GenericBlockedCursor<Codec, true>::size_type;
    using value_type = typename GenericBlockedCursor<Codec, true>::value_type;

    DocumentBlockedCursor(gsl::span<std::byte const> encoded_blocks,
                          UnalignedSpan<offset_type const> block_endpoints,
                          UnalignedSpan<value_type const> block_last_values,
                          std::uint32_t length,
                          std::uint32_t num_blocks)
        : GenericBlockedCursor<Codec, true>(
            encoded_blocks, block_endpoints, block_last_values, length, num_blocks)
    {
    }

    /// Moves the cursor to the next value equal or greater than `value`.
    void advance_to_geq(value_type value)
    {
        auto& current_block = this->current_block();
        if (PISA_UNLIKELY(value > this->current_block_last_value())) {
            if (value > this->block_last_values().back()) {
                this->update_current_value(this->sentinel());
                return;
            }
            auto block = current_block.number + 1U;
            while (this->block_last_values()[block] < value) {
                ++block;
            }
            this->decode_and_update_block(block);
        }

        while (this->value() < value) {
            this->increase_current_value(this->decoded_value(++current_block.offset));
            Ensures(current_block.offset < current_block.length);
        }
    }
};

template <typename Codec>
struct PayloadBlockedCursor : public GenericBlockedCursor<Codec, false> {
    using offset_type = typename GenericBlockedCursor<Codec, false>::offset_type;
    using size_type = typename GenericBlockedCursor<Codec, false>::size_type;
    using value_type = typename GenericBlockedCursor<Codec, false>::value_type;

    PayloadBlockedCursor(gsl::span<std::byte const> encoded_blocks,
                         UnalignedSpan<offset_type const> block_endpoints,
                         std::uint32_t length,
                         std::uint32_t num_blocks)
        : GenericBlockedCursor<Codec, false>(
            encoded_blocks, block_endpoints, {}, length, num_blocks)
    {
    }
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
struct GenericBlockedReader {
    using value_type = std::uint32_t;

    void init(BaseIndex const& index) {}
    [[nodiscard]] auto read(gsl::span<const std::byte> bytes) const
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
            return DocumentBlockedCursor<Codec>(
                encoded_blocks, block_endpoints, block_last_values, length, num_blocks);
        } else {
            return PayloadBlockedCursor<Codec>(encoded_blocks, block_endpoints, length, num_blocks);
        }
    }

    constexpr static auto encoding() -> std::uint32_t
    {
        return block_encoding_type<DeltaEncoded>()
               | encoding_traits<Codec>::encoding_tag::encoding();
    }
};

template <typename Codec>
using DocumentBlockedReader = GenericBlockedReader<Codec, true>;
template <typename Codec>
using PayloadBlockedReader = GenericBlockedReader<Codec, false>;

template <typename Codec, bool DeltaEncoded>
struct GenericBlockedWriter {
    using value_type = std::uint32_t;

    GenericBlockedWriter() = default;
    explicit GenericBlockedWriter([[maybe_unused]] std::size_t num_documents) {}

    constexpr static auto encoding() -> std::uint32_t
    {
        return block_encoding_type<DeltaEncoded>()
               | encoding_traits<Codec>::encoding_tag::encoding();
    }

    void init([[maybe_unused]] pisa::binary_freq_collection const& collection) {}
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

template <typename Codec>
using DocumentBlockedWriter = GenericBlockedWriter<Codec, true>;
template <typename Codec>
using PayloadBlockedWriter = GenericBlockedWriter<Codec, false>;

template <typename Codec>
struct CursorTraits<DocumentBlockedCursor<Codec>> {
    using Writer = DocumentBlockedWriter<Codec>;
    using Reader = DocumentBlockedReader<Codec>;
};

template <typename Codec>
struct CursorTraits<PayloadBlockedCursor<Codec>> {
    using Writer = PayloadBlockedWriter<Codec>;
    using Reader = PayloadBlockedReader<Codec>;
};

} // namespace pisa::v1
