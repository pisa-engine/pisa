// Copyright 2024 PISA developers
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <concepts>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <ostream>
#include <span>

namespace pisa::lt {

namespace detail {

    class BaseLookupTable {
      public:
        virtual ~BaseLookupTable() = default;
        [[nodiscard]] virtual auto size() const noexcept -> std::size_t = 0;
        [[nodiscard]] virtual auto operator[](std::size_t idx) const
            -> std::span<std::byte const> = 0;
        [[nodiscard]] virtual auto find(std::span<std::byte const> value) const noexcept
            -> std::optional<std::size_t> = 0;

        [[nodiscard]] virtual auto clone() -> std::unique_ptr<BaseLookupTable> = 0;
    };

    class BaseLookupTableEncoder {
      public:
        virtual ~BaseLookupTableEncoder() = default;
        void virtual insert(std::span<std::byte const> payload) = 0;
        void virtual encode(std::ostream& out) = 0;
    };

}  // namespace detail

namespace v1 {

    class Flags {
      private:
        std::uint8_t flags = 0;

      public:
        constexpr Flags() = default;
        explicit constexpr Flags(std::uint8_t bitset) : flags(bitset) {}

        [[nodiscard]] auto sorted() const noexcept -> bool;
        [[nodiscard]] auto wide_offsets() const noexcept -> bool;
        [[nodiscard]] auto bits() const noexcept -> std::uint8_t;
    };

    namespace flags {
        inline constexpr std::uint8_t SORTED = 0b001;
        inline constexpr std::uint8_t WIDE_OFFSETS = 0b010;
    }  // namespace flags

};  // namespace v1

}  // namespace pisa::lt

namespace pisa {

/**
 * Lookup table mapping integers from a range [0, N) to binary payloads.
 *
 * This table assigns each _unique_ value (duplicates are not allowed) to an index in [0, N), where
 * N is the size of the table. Thus, this structure is equivalent to a sequence of binary values.
 * The difference between `LookupTable` and, say, `std::vector` is that its encoding supports
 * reading the values without fully parsing the entire binary representation of the table. As such,
 * it supports quickly initializing the structure from an external device (with random access),
 * e.g., via mmap, and performing a lookup without loading the entire structure to main memory.
 * This is especially useful for short-lived programs that must perform a lookup without the
 * unnecessary overhead of loading it to memory.
 *
 * If the values are sorted, and the appropriate flag is toggled in the header, a quick binary
 * search lookup can be performed to find an index of a value. If the values are not sorted, then a
 * linear scan will be used; therefore, one should consider having values sorted if such lookups are
 * important. Getting the value at a given index is a constant-time operation, though if using
 * memory mapping, each such operation may need to load multiple pages to memory.
 */
class LookupTable {
  private:
    std::unique_ptr<::pisa::lt::detail::BaseLookupTable> m_impl;

    explicit LookupTable(std::unique_ptr<::pisa::lt::detail::BaseLookupTable> impl);

    [[nodiscard]] static auto v1(std::span<const std::byte> bytes) -> LookupTable;

  public:
    LookupTable(LookupTable const&);
    LookupTable(LookupTable&&);
    LookupTable& operator=(LookupTable const&);
    LookupTable& operator=(LookupTable&&);
    ~LookupTable();

    /**
     * The number of elements in the table.
     */
    [[nodiscard]] auto size() const noexcept -> std::size_t;

    /**
     * Retrieves the value at index `idx`.
     *
     * If `idx < size()`, then `std::out_of_range` exception is thrown. See `at()` if you want to
     * conveniently cast the memory span to another type.
     */
    [[nodiscard]] auto operator[](std::size_t idx) const -> std::span<std::byte const>;

    /**
     * Returns the position of `value` in the table or `std::nullopt` if the value does not exist.
     *
     * See the templated version of this function if you want to automatically cast from another
     * type to byte span.
     */
    [[nodiscard]] auto find(std::span<std::byte const> value) const noexcept
        -> std::optional<std::size_t>;

    /**
     * Returns the value at index `idx` cast to type `T`.
     *
     * The type `T` must define `T::value_type` that resolves to a byte-wide type, as well as a
     * constructor that takes `T::value_type const*` (pointer to the first byte) and `std::size_t`
     * (the total number of bytes). If `T::value_type` is longer than 1 byte, this operation results
     * in **undefined behavior**.
     *
     * Examples of types that can be used are: `std::string_view` or `std::span<const char>`.
     */
    template <typename T>
    [[nodiscard]] auto at(std::size_t idx) const -> T {
        auto bytes = this->operator[](idx);
        return T(reinterpret_cast<typename T::value_type const*>(bytes.data()), bytes.size());
    }

    /**
     * Returns the position of `value` in the table or `std::nullopt` if the value does not exist.
     *
     * The type `T` of the value must be such that `std:span<typename T::value_type const>` is
     * constructible from `T`.
     */
    template <typename T>
        requires(std::constructible_from<std::span<typename T::value_type const>, T>)
    [[nodiscard]] auto find(T value) const noexcept -> std::optional<std::size_t> {
        return find(std::as_bytes(std::span<typename T::value_type const>(value)));
    }

    /**
     * Constructs a lookup table from the encoded sequence of bytes.
     */
    [[nodiscard]] static auto from_bytes(std::span<std::byte const> bytes) -> LookupTable;
};

/**
 * Lookup table encoder.
 *
 * This class builds and encodes a sequence of values to the binary format of lookup table.
 * See `LookupTable` for more details.
 *
 * Note that all encoded data is accumulated in memory and only flushed to the output stream when
 * `encode()` member function is called.
 */
class LookupTableEncoder {
    std::unique_ptr<::pisa::lt::detail::BaseLookupTableEncoder> m_impl;

    explicit LookupTableEncoder(std::unique_ptr<::pisa::lt::detail::BaseLookupTableEncoder> impl);

  public:
    /**
     * Constructs an encoder for a lookup table in v1 format, with the given flag options.
     *
     * If sorted flag is _not_ set, then an additional hash set will be produced to keep track of
     * duplicates. This will increase the memory footprint at build time.
     */
    static LookupTableEncoder v1(::pisa::lt::v1::Flags flags);

    /**
     * Inserts payload.
     *
     * If sorted flag was set at construction time, it will throw if the given payload is not
     * lexicographically greater than the previously inserted payload. If sorted flag was _not_ set
     * and the given payload has already been inserted, it will throw as well.
     */
    auto insert(std::span<std::byte const> payload) -> LookupTableEncoder&;

    /**
     * Writes the encoded table to the output stream.
     */
    auto encode(std::ostream& out) -> LookupTableEncoder&;

    /**
     * Inserts a payload of type `Payload`.
     *
     * `std::span<typename Payload::value_type const>` must be constructible from `Payload`, which
     * in turn will be cast as byte span before calling the non-templated version of `insert()`.
     */
    template <typename Payload>
        requires(std::constructible_from<std::span<typename Payload::value_type const>, Payload>)
    auto insert(Payload const& payload) -> LookupTableEncoder& {
        insert(std::as_bytes(std::span(payload)));
        return *this;
    }

    /**
     * Inserts all payloads in the given span.
     *
     * It calls `insert()` for each element in the span. See `insert()` for more details.
     */
    template <typename Payload>
    auto insert_span(std::span<Payload const> payloads) -> LookupTableEncoder& {
        for (auto const& payload: payloads) {
            insert(payload);
        }
        return *this;
    }
};

}  // namespace pisa
