#pragma once

#include <utility>

#include "v1/cursor_traits.hpp"

namespace pisa::v1 {

template <typename Cursor, typename Label>
struct LabeledCursor {
    using Value = typename CursorTraits<Cursor>::Value;

    explicit constexpr LabeledCursor(Cursor cursor, Label label)
        : m_cursor(std::move(cursor)), m_label(std::move(label))
    {
    }

    [[nodiscard]] constexpr auto operator*() const -> Value { return value(); }
    [[nodiscard]] constexpr auto value() const noexcept -> Value { return m_cursor.value(); }
    [[nodiscard]] constexpr auto payload() noexcept { return m_cursor.payload(); }
    constexpr void advance() { m_cursor.advance(); }
    constexpr void advance_to_position(std::size_t pos) { m_cursor.advance_to_position(pos); }
    constexpr void advance_to_geq(Value value) { m_cursor.advance_to_geq(value); }
    [[nodiscard]] constexpr auto empty() const noexcept -> bool { return m_cursor.empty(); }
    [[nodiscard]] constexpr auto position() const noexcept -> std::size_t
    {
        return m_cursor.position();
    }
    // TODO: Support not sized
    [[nodiscard]] constexpr auto size() const -> std::size_t { return m_cursor.size(); }
    [[nodiscard]] constexpr auto sentinel() const -> Value { return m_cursor.sentinel(); }
    [[nodiscard]] constexpr auto label() const -> Label const& { return m_label; }
    [[nodiscard]] constexpr auto max_score() const { return m_cursor.max_score(); }

   private:
    Cursor m_cursor;
    Label m_label;
};

template <typename Cursor, typename Label>
auto label(Cursor cursor, Label label)
{
    return LabeledCursor<Cursor, Label>(std::move(cursor), std::move(label));
}

template <typename Cursor, typename LabelFn>
auto label(std::vector<Cursor> cursors, LabelFn&& label_fn)
{
    using label_type = std::decay_t<decltype(label_fn(std::declval<Cursor>()))>;
    std::vector<LabeledCursor<Cursor, label_type>> labeled;
    std::transform(cursors.begin(),
                   cursors.end(),
                   std::back_inserter(labeled),
                   [&](Cursor&& cursor) { return label(cursor, label_fn(cursor)); });
    return labeled;
}

template <typename Cursor, typename Label>
struct CursorTraits<LabeledCursor<Cursor, Label>> {
    using Value = typename CursorTraits<Cursor>::Value;
};

} // namespace pisa::v1
