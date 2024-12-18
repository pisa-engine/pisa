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

#include <concepts>
#include <cstdint>
#include <cstring>
#include <numeric>
#include <optional>
#include <span>
#include <unordered_set>
#include <vector>

#include <fmt/format.h>
#include <string>

#include "fmt/core.h"
#include "pisa/lookup_table.hpp"
#include "pisa/span.hpp"
#include "pisa/stream.hpp"

namespace pisa::lt {

constexpr std::byte VERIFICATION_BYTE = std::byte(0x87);
constexpr std::size_t PADDING_LENGTH = 5;
constexpr std::array<std::byte, PADDING_LENGTH> PADDING = {
    std::byte{0}, std::byte{0}, std::byte{0}, std::byte{0}, std::byte{0}
};

auto v1::Flags::sorted() const noexcept -> bool {
    return (this->flags & 0b1) > 0;
}

auto v1::Flags::wide_offsets() const noexcept -> bool {
    return (this->flags & 0b10) > 0;
}

auto v1::Flags::bits() const noexcept -> std::uint8_t {
    return this->flags;
}

}  // namespace pisa::lt

namespace pisa {

LookupTable::LookupTable(std::unique_ptr<::pisa::lt::detail::BaseLookupTable> impl)
    : m_impl(std::move(impl)) {}

LookupTable::LookupTable(LookupTable const& other) : m_impl(other.m_impl->clone()) {}

LookupTable::LookupTable(LookupTable&&) = default;

LookupTable& LookupTable::operator=(LookupTable const& other) {
    m_impl = other.m_impl->clone();
    return *this;
}

LookupTable& LookupTable::operator=(LookupTable&&) = default;

LookupTable::~LookupTable() = default;

template <typename T>
    requires(std::unsigned_integral<T>)
[[nodiscard]] auto
read(std::span<std::byte const> bytes, std::size_t offset, std::string const& error_msg) -> T {
    auto sub = pisa::subspan_or_throw(bytes, offset, sizeof(T), error_msg);
    T value;
    std::memcpy(&value, bytes.data() + offset, sizeof(T));
    return value;
}

template <typename T>
    requires(std::unsigned_integral<T>)
[[nodiscard]] auto read(std::span<std::byte const> bytes, std::size_t offset) -> T {
    return read<T>(bytes, offset, "not enough bytes");
}

void validate_padding(std::span<std::byte const> bytes) {
    auto padding = read<std::uint64_t>(bytes, 0, "not enough bytes for header");
    padding &= 0xFFFFFFFFFF000000;
    if (padding != 0) {
        throw std::domain_error(fmt::format(
            "bytes 3-7 must be all 0 but are {:#2x} {:#2x} {:#2x} {:#2x} {:#2x}",
            bytes[3],
            bytes[4],
            bytes[5],
            bytes[6],
            bytes[7]
        ));
    }
}

template <typename Offset>
class LookupTableV1: public ::pisa::lt::detail::BaseLookupTable {
    std::span<std::byte const> m_offsets;
    std::span<std::byte const> m_payloads;
    std::size_t m_size;
    bool m_sorted;

    [[nodiscard]] auto read_offset(std::size_t idx) const -> Offset {
        return read<Offset>(m_offsets, idx * sizeof(Offset));
    }

    [[nodiscard]] auto read_payload(std::size_t idx) const -> std::span<std::byte const> {
        auto offset = read_offset(idx);
        auto count = read_offset(idx + 1) - offset;
        return pisa::subspan_or_throw(m_payloads, offset, count, "not enough bytes for payload");
    }

  public:
    LookupTableV1(std::span<const std::byte> offsets, std::span<const std::byte> payloads, bool sorted)
        : m_offsets(offsets),
          m_payloads(payloads),
          m_size(m_offsets.size() / sizeof(Offset) - 1),
          m_sorted(sorted) {}

    ~LookupTableV1() = default;

    [[nodiscard]] virtual auto clone() -> std::unique_ptr<BaseLookupTable> override {
        return std::make_unique<LookupTableV1<Offset>>(m_offsets, m_payloads, m_sorted);
    }

    [[nodiscard]] virtual auto size() const noexcept -> std::size_t override { return m_size; }

    [[nodiscard]] virtual auto operator[](std::size_t idx) const
        -> std::span<std::byte const> override {
        if (idx >= m_size) {
            throw std::out_of_range(
                fmt::format("accessing element {} in a table of size {}", idx, m_size)
            );
        }
        auto offset = read_offset(idx);
        auto count = read_offset(idx + 1) - offset;
        return pisa::subspan_or_throw(m_payloads, offset, count, "not enough bytes for payload");
    }

    [[nodiscard]] virtual auto find_sorted(std::span<std::byte const> value) const noexcept
        -> std::optional<std::size_t> {
        if (size() == 0) {
            return std::nullopt;
        }
        std::size_t low = 0;
        std::size_t high = size() - 1;
        while (low < high) {
            auto mid = std::midpoint(low, high);
            auto midval = read_payload(mid);
            if (lex_lt(midval, value)) {
                low = mid + 1;
            } else {
                high = mid;
            }
        }
        return lex_eq(value, read_payload(low)) ? std::optional(low) : std::nullopt;
    }

    [[nodiscard]] virtual auto find_unsorted(std::span<std::byte const> value) const noexcept
        -> std::optional<std::size_t> {
        for (std::size_t pos = 0; pos < size(); ++pos) {
            if (read_payload(pos) == value) {
                return pos;
            }
        }
        return std::nullopt;
    }

    [[nodiscard]] virtual auto find(std::span<std::byte const> value) const noexcept
        -> std::optional<std::size_t> override {
        return m_sorted ? find_sorted(value) : find_unsorted(value);
    }
};

template <typename Offset>
auto construct_lookup_table_v1(std::span<std::byte const> bytes, bool sorted)
    -> std::unique_ptr<::pisa::lt::detail::BaseLookupTable> {
    auto length = read<std::uint64_t>(bytes, 8, "not enough bytes for table length");
    std::size_t offsets_bytes_length = (length + 1) * sizeof(Offset);
    auto offsets =
        pisa::subspan_or_throw(bytes, 16, offsets_bytes_length, "not enough bytes for offsets");
    auto payloads = pisa::subspan_or_throw(bytes, 16 + offsets_bytes_length, std::dynamic_extent);
    return std::make_unique<LookupTableV1<Offset>>(offsets, payloads, sorted);
}

auto LookupTable::v1(std::span<std::byte const> bytes) -> LookupTable {
    validate_padding(bytes);
    auto flags = lt::v1::Flags(static_cast<std::uint8_t>(bytes[2]));
    if (flags.wide_offsets()) {
        return LookupTable(construct_lookup_table_v1<std::uint64_t>(bytes, flags.sorted()));
    }
    return LookupTable(construct_lookup_table_v1<std::uint32_t>(bytes, flags.sorted()));
}

auto LookupTable::from_bytes(std::span<std::byte const> bytes) -> LookupTable {
    auto leading_bytes = pisa::subspan_or_throw(bytes, 0, 2, "header must be at least 2 bytes");
    auto verification_byte = leading_bytes[0];
    if (verification_byte != lt::VERIFICATION_BYTE) {
        throw std::domain_error(fmt::format(
            "lookup table verification byte invalid: must be {:#x} but {:#x} given",
            lt::VERIFICATION_BYTE,
            verification_byte
        ));
    }

    auto version = static_cast<std::uint8_t>(leading_bytes[1]);
    if (version != 1) {
        throw std::domain_error(fmt::format("only version 1 is valid but {} given", version));
    }

    return LookupTable::v1(bytes);
}

auto LookupTable::size() const noexcept -> std::size_t {
    return m_impl->size();
}
auto LookupTable::operator[](std::size_t idx) const -> std::span<std::byte const> {
    return m_impl->operator[](idx);
}

auto LookupTable::find(std::span<std::byte const> value) const noexcept
    -> std::optional<std::size_t> {
    return m_impl->find(value);
}

template <typename Offset>
class LookupTableEncoderV1: public ::pisa::lt::detail::BaseLookupTableEncoder {
    ::pisa::lt::v1::Flags m_flags;
    std::vector<Offset> m_offsets{0};
    std::vector<std::byte> m_payloads{};
    std::unordered_set<std::string_view> m_inserted{};
    std::span<std::byte const> m_prev_payload;

    void encode_header(std::ostream& out) {
        auto flag_bits = m_flags.bits();
        pisa::put(out, static_cast<char>(lt::VERIFICATION_BYTE));
        pisa::put(out, static_cast<char>(1));
        pisa::put(out, static_cast<char>(flag_bits));
        pisa::write(
            out, reinterpret_cast<char const*>(&::pisa::lt::PADDING), ::pisa::lt::PADDING_LENGTH
        );
    }

    void write_offsets(std::ostream& out) {
        for (auto const& offset: m_offsets) {
            pisa::write(out, reinterpret_cast<char const*>(&offset), sizeof(Offset));
        }
    }

  public:
    explicit LookupTableEncoderV1(::pisa::lt::v1::Flags flags) : m_flags(flags) {}

    virtual ~LookupTableEncoderV1() = default;

    void virtual insert(std::span<std::byte const> payload) {
        if (m_flags.sorted()) {
            if (!pisa::lex_lt<std::byte const>(m_prev_payload, payload)) {
                throw std::invalid_argument("payloads not strictly sorted in sorted table");
            }
        } else {
            auto payload_as_str =
                std::string_view(reinterpret_cast<char const*>(payload.data()), payload.size());
            if (auto pos = m_inserted.find(payload_as_str); pos != m_inserted.end()) {
                throw std::invalid_argument("payload duplicate");
            }
            m_inserted.insert(payload_as_str);
        }
        auto prev_begin = m_offsets.back();
        m_offsets.push_back(m_offsets.back() + payload.size());
        m_payloads.insert(m_payloads.end(), payload.begin(), payload.end());
        m_prev_payload = std::span(m_payloads).subspan(prev_begin, payload.size());
    }

    void virtual encode(std::ostream& out) {
        encode_header(out);
        std::uint64_t size = m_offsets.size() - 1;
        pisa::write(out, reinterpret_cast<char const*>(&size), sizeof(size));
        write_offsets(out);
        pisa::write(out, reinterpret_cast<char const*>(m_payloads.data()), m_payloads.size());
    }
};

LookupTableEncoder::LookupTableEncoder(std::unique_ptr<::pisa::lt::detail::BaseLookupTableEncoder> impl)
    : m_impl(std::move(impl)) {}

LookupTableEncoder LookupTableEncoder::v1(::pisa::lt::v1::Flags flags) {
    if (flags.wide_offsets()) {
        return LookupTableEncoder(std::make_unique<LookupTableEncoderV1<std::uint64_t>>(flags));
    }
    return LookupTableEncoder(std::make_unique<LookupTableEncoderV1<std::uint32_t>>(flags));
}

auto LookupTableEncoder::insert(std::span<std::byte const> payload) -> LookupTableEncoder& {
    m_impl->insert(payload);
    return *this;
}

auto LookupTableEncoder::encode(std::ostream& out) -> LookupTableEncoder& {
    m_impl->encode(out);
    return *this;
}

}  // namespace pisa
