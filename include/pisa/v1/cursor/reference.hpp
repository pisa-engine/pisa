#pragma once

#include <utility>

#include <gsl/span>

#include "v1/cursor_traits.hpp"

namespace pisa::v1 {

template <typename Cursor>
struct CursorRef {
    using Value = typename CursorTraits<std::decay_t<Cursor>>::Value;

    constexpr CursorRef(Cursor&& cursor) : m_cursor(std::ref(cursor)) {}
    [[nodiscard]] constexpr auto operator*() const -> Value { return value(); }
    [[nodiscard]] constexpr auto value() const noexcept -> Value { return m_cursor.get().value(); }
    [[nodiscard]] constexpr auto payload() { return m_cursor.get().payload(); }
    constexpr void advance() { m_cursor.get().advance(); }
    constexpr void advance_to_geq(Value value) { m_cursor.get().advance_to_geq(value); }
    constexpr void advance_to_position(std::size_t pos) { m_cursor.get().advance_to_position(pos); }
    [[nodiscard]] constexpr auto empty() const noexcept -> bool { return m_cursor.get().empty(); }
    [[nodiscard]] constexpr auto position() const noexcept -> std::size_t
    {
        return m_cursor.get().position();
    }
    [[nodiscard]] constexpr auto size() const -> std::size_t { return m_cursor.get().size(); }
    [[nodiscard]] constexpr auto max_score() const { return m_cursor.get().max_score(); }

   private:
    std::reference_wrapper<std::decay_t<Cursor>> m_cursor;
};

template <typename Cursor>
auto ref(Cursor&& cursor)
{
    return CursorRef<Cursor>(std::forward<Cursor>(cursor));
}

template <typename Cursor>
struct CursorTraits<CursorRef<Cursor>> {
    using Value = typename CursorTraits<Cursor>::Value;
};

} // namespace pisa::v1
