#pragma once

#include <cstdint>
#include <type_traits>

#include <gsl/gsl_assert>
#include <gsl/span>

#include "v1/bit_cast.hpp"

namespace pisa::v1 {

template <typename T>
struct UnalignedSpan;

template <typename T>
struct UnalignedSpanIterator {
    UnalignedSpanIterator(std::uint32_t index, UnalignedSpan<T> const& span)
        : m_index(index), m_span(span)
    {
    }
    UnalignedSpanIterator(UnalignedSpanIterator const&) = default;
    UnalignedSpanIterator(UnalignedSpanIterator&&) noexcept = default;
    UnalignedSpanIterator& operator=(UnalignedSpanIterator const&) = default;
    UnalignedSpanIterator& operator=(UnalignedSpanIterator&&) noexcept = default;
    ~UnalignedSpanIterator() = default;
    [[nodiscard]] auto operator==(UnalignedSpanIterator<T> const& other) const
    {
        return m_span.bytes().data() == other.m_span.bytes().data() && m_index == other.m_index;
    }
    [[nodiscard]] auto operator!=(UnalignedSpanIterator<T> const& other) const
    {
        return m_index != other.m_index || m_span.bytes().data() != other.m_span.bytes().data();
    }
    [[nodiscard]] auto operator*() const { return m_span[m_index]; }
    auto operator++() -> UnalignedSpanIterator&
    {
        m_index++;
        return *this;
    }
    auto operator++(int) -> UnalignedSpanIterator
    {
        auto copy = *this;
        m_index++;
        return copy;
    }
    [[nodiscard]] auto operator+=(std::uint32_t n) -> UnalignedSpanIterator&
    {
        m_index += n;
        return *this;
    }
    [[nodiscard]] auto operator+(std::uint32_t n) const -> UnalignedSpanIterator
    {
        return UnalignedSpanIterator(m_index + n, m_span);
    }
    auto operator--() -> UnalignedSpanIterator&
    {
        m_index--;
        return *this;
    }
    auto operator--(int) -> UnalignedSpanIterator
    {
        auto copy = *this;
        m_index--;
        return copy;
    }
    [[nodiscard]] auto operator-=(std::uint32_t n) -> UnalignedSpanIterator&
    {
        m_index -= n;
        return *this;
    }
    [[nodiscard]] auto operator-(std::uint32_t n) const -> UnalignedSpanIterator
    {
        return UnalignedSpanIterator(m_index - n, m_span);
    }
    [[nodiscard]] auto operator-(UnalignedSpanIterator const& other) const -> std::int32_t
    {
        return static_cast<std::int32_t>(m_index) - static_cast<std::int32_t>(other.m_index);
    }
    [[nodiscard]] auto operator<(UnalignedSpanIterator const& other) const -> bool
    {
        return m_index < other.m_index;
    }
    [[nodiscard]] auto operator<=(UnalignedSpanIterator const& other) const -> bool
    {
        return m_index <= other.m_index;
    }
    [[nodiscard]] auto operator>(UnalignedSpanIterator const& other) const -> bool
    {
        return m_index > other.m_index;
    }
    [[nodiscard]] auto operator>=(UnalignedSpanIterator const& other) const -> bool
    {
        return m_index >= other.m_index;
    }

   private:
    std::uint32_t m_index;
    UnalignedSpan<T> const& m_span;
};

template <typename T>
struct UnalignedSpan {
    static_assert(std::is_trivially_copyable_v<T>);
    using value_type = T;

    constexpr UnalignedSpan() = default;
    explicit constexpr UnalignedSpan(gsl::span<std::byte const> bytes) : m_bytes(bytes)
    {
        if (m_bytes.size() % sizeof(value_type) != 0) {
            throw std::logic_error("Number of bytes must be a multiplier of type size");
        }
    }
    constexpr UnalignedSpan(UnalignedSpan const&) = default;
    constexpr UnalignedSpan(UnalignedSpan&&) noexcept = default;
    constexpr UnalignedSpan& operator=(UnalignedSpan const&) = default;
    constexpr UnalignedSpan& operator=(UnalignedSpan&&) noexcept = default;
    ~UnalignedSpan() = default;

    using iterator = UnalignedSpanIterator<T>;

    [[nodiscard]] auto operator[](std::uint32_t index) const -> value_type
    {
        return bit_cast<value_type>(
            m_bytes.subspan(index * sizeof(value_type), sizeof(value_type)));
    }

    [[nodiscard]] auto front() const -> value_type
    {
        return bit_cast<value_type>(m_bytes.subspan(0, sizeof(value_type)));
    }

    [[nodiscard]] auto back() const -> value_type
    {
        return bit_cast<value_type>(
            m_bytes.subspan(m_bytes.size() - sizeof(value_type), sizeof(value_type)));
    }

    [[nodiscard]] auto begin() const -> iterator { return iterator(0, *this); }
    [[nodiscard]] auto end() const -> iterator { return iterator(size(), *this); }

    [[nodiscard]] auto size() const -> std::size_t { return m_bytes.size() / sizeof(value_type); }
    [[nodiscard]] auto byte_size() const -> std::size_t { return m_bytes.size(); }
    [[nodiscard]] auto bytes() const -> gsl::span<std::byte const> { return m_bytes; }
    [[nodiscard]] auto empty() const -> bool { return m_bytes.empty(); }

   private:
    gsl::span<std::byte const> m_bytes{};
};

} // namespace pisa::v1

namespace std {

template <typename T>
struct iterator_traits<::pisa::v1::UnalignedSpanIterator<T>> {
    using size_type = std::uint32_t;
    using difference_type = std::make_signed_t<size_type>;
    using value_type = T;
    using pointer = T const*;
    using reference = T const&;
    using iterator_category = std::random_access_iterator_tag;
};

} // namespace std
