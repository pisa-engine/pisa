#pragma once

#include <vector>

#include "query.hpp"
#include "util/compiler_attribute.hpp"

namespace pisa {

template <typename Cursor, typename T = std::size_t>
class NumberedCursor: public Cursor {
  public:
    using base_cursor_type = Cursor;

    NumberedCursor(Cursor cursor, T term_position)
        : base_cursor_type(std::move(cursor)), m_position(term_position)
    {}

    [[nodiscard]] PISA_ALWAYSINLINE auto term_position() const -> T const& { return m_position; }

  private:
    T m_position;
};

template <typename Cursor, typename T>
[[nodiscard]] auto number_cursor(Cursor cursor, T position)
{
    return NumberedCursor<Cursor, T>(std::move(cursor), position);
}

template <typename Cursor>
[[nodiscard]] auto number_cursors(std::vector<Cursor> cursors)
{
    std::vector<NumberedCursor<Cursor>> numbered_cursors;
    numbered_cursors.reserve(cursors.size());
    std::size_t position = 0;
    for (auto&& cursor: cursors) {
        numbered_cursors.emplace_back(std::move(cursor), position++);
    }
    return numbered_cursors;
}

template <typename Cursor, typename T>
[[nodiscard]] auto number_cursors(std::vector<Cursor> cursors, std::vector<T> const& positions)
{
    if (cursors.size() != positions.size()) {
        throw std::invalid_argument("Number of cursors must match number of positions");
    }
    std::vector<NumberedCursor<Cursor>> numbered_cursors;
    numbered_cursors.reserve(cursors.size());
    auto position = positions.begin();
    for (auto&& cursor: cursors) {
        numbered_cursors.emplace_back(std::move(cursor), *position++);
    }
    return numbered_cursors;
}

}  // namespace pisa
