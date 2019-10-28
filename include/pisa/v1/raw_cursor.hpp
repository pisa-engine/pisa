#pragma once

#include <cstdint>
#include <cstring>
#include <type_traits>
#include <utility>

#include <gsl/gsl_assert>
#include <gsl/span>
#include <tl/optional.hpp>

#include "util/likely.hpp"
#include "v1/bit_cast.hpp"
#include "v1/types.hpp"

namespace pisa::v1 {

/// Uncompressed example of implementation of a single value cursor.
template <typename T>
struct RawCursor {
    static_assert(std::is_trivially_copyable_v<T>);
    using value_type = T;

    /// Creates a cursor from the encoded bytes.
    explicit constexpr RawCursor(gsl::span<const std::byte> bytes);

    /// Dereferences the current value.
    /// It is an undefined behavior to call this when `empty() == true`.
    [[nodiscard]] constexpr auto operator*() const -> T;

    /// Safely returns the current value, or returns `nullopt` if `empty() == true`.
    [[nodiscard]] constexpr auto value() const noexcept -> tl::optional<T>;

    /// Moves the cursor to the next position.
    constexpr void step();

    /// Moves the cursor to the position `pos`.
    constexpr void step_to_position(std::size_t pos);

    /// Moves the cursor to the next value equal or greater than `value`.
    constexpr void step_to_geq(T value);

    /// This is semantically equivalent to first calling `step()` and then `value()`.
    constexpr auto next() -> tl::optional<T>;

    /// Returns `true` if there is no elements left.
    [[nodiscard]] constexpr auto empty() const noexcept -> bool;

    /// Returns the current position.
    [[nodiscard]] constexpr auto position() const noexcept -> std::size_t;

    /// Returns the number of elements in the list.
    [[nodiscard]] constexpr auto size() const -> std::size_t;

    /// The sentinel value, such that `value() != nullopt` is equivalent to `*(*this) < sentinel()`.
    [[nodiscard]] constexpr auto sentinel() const -> T;

   private:
    std::size_t m_current = 0;
    gsl::span<const std::byte> m_bytes;
};

template <typename T>
constexpr RawCursor<T>::RawCursor(gsl::span<const std::byte> bytes) : m_bytes(bytes.subspan(4))
{
    Expects(m_bytes.size() % sizeof(T) == 0);
}

template <typename T>
[[nodiscard]] constexpr auto RawCursor<T>::operator*() const -> T
{
    if (PISA_UNLIKELY(empty())) {
        return sentinel();
    }
    return bit_cast<T>(gsl::as_bytes(m_bytes.subspan(m_current, sizeof(T))));
}

template <typename T>
[[nodiscard]] constexpr auto RawCursor<T>::sentinel() const -> T
{
    return std::numeric_limits<T>::max();
}

template <typename T>
[[nodiscard]] constexpr auto RawCursor<T>::value() const noexcept -> tl::optional<T>
{
    return empty() ? tl::nullopt : tl::make_optional(*(*this));
}
template <typename T>
constexpr auto RawCursor<T>::next() -> tl::optional<T>
{
    step();
    return value();
}
template <typename T>
constexpr void RawCursor<T>::step()
{
    m_current += sizeof(T);
}

template <typename T>
[[nodiscard]] constexpr auto RawCursor<T>::empty() const noexcept -> bool
{
    return m_current == m_bytes.size();
}

template <typename T>
[[nodiscard]] constexpr auto RawCursor<T>::position() const noexcept -> std::size_t
{
    return m_current;
}

template <typename T>
[[nodiscard]] constexpr auto RawCursor<T>::size() const -> std::size_t
{
    return m_bytes.size() / sizeof(T);
}

template <typename T>
constexpr void RawCursor<T>::step_to_position(std::size_t pos)
{
    m_current = pos;
}

template <typename T>
constexpr void RawCursor<T>::step_to_geq(T value)
{
    while (not empty() && *(*this) < value) {
        step();
    }
}

template <typename T>
struct RawReader {
    static_assert(std::is_trivially_copyable<T>::value);
    using value_type = T;

    [[nodiscard]] auto read(gsl::span<const std::byte> bytes) const -> RawCursor<T>
    {
        return RawCursor<T>(bytes);
    }

    constexpr static auto encoding() -> std::uint32_t { return EncodingId::Raw; }
};

template <typename T>
struct RawWriter {
    static_assert(std::is_trivially_copyable<T>::value);
    using value_type = T;

    constexpr static auto encoding() -> std::uint32_t { return EncodingId::Raw; }

    void push(T const &posting) { m_postings.push_back(posting); }
    void push(T &&posting) { m_postings.push_back(posting); }

    [[nodiscard]] auto write(std::ostream &os) const -> std::size_t
    {
        assert(!m_postings.empty());
        std::uint32_t length = m_postings.size();
        os.write(reinterpret_cast<char const *>(&length), sizeof(length));
        auto memory = gsl::as_bytes(gsl::make_span(m_postings.data(), m_postings.size()));
        os.write(reinterpret_cast<char const *>(memory.data()), memory.size());
        return sizeof(length) + memory.size();
    }

    template <typename OutputByteIterator>
    auto append(OutputByteIterator out) const -> OutputByteIterator
    {
        assert(!m_postings.empty());
        std::uint32_t length = m_postings.size();
        auto length_bytes = gsl::as_bytes(gsl::make_span(&length, 1));
        auto memory = gsl::as_bytes(gsl::make_span(m_postings.data(), m_postings.size()));
        std::copy(length_bytes.begin(), length_bytes.end(), out);
        std::copy(memory.begin(), memory.end(), out);
        return out;
    }
    void reset() { m_postings.clear(); }

   private:
    std::vector<T> m_postings;
};

} // namespace pisa::v1
