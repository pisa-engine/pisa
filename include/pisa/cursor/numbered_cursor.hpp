#pragma once

#include <vector>

#include "query.hpp"
#include "util/compiler_attribute.hpp"

namespace pisa {

template <typename Cursor>
class NumberedCursor: public Cursor {
  public:
    using base_cursor_type = Cursor;

    NumberedCursor(Cursor cursor, std::size_t term_position)
        : base_cursor_type(std::move(cursor)), m_position(term_position)
    {}

    [[nodiscard]] PISA_ALWAYSINLINE auto term_position() const -> std::size_t { return m_position; }

  private:
    std::size_t m_position;
};

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

}  // namespace pisa
