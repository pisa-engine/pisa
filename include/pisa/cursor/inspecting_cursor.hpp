#pragma once

#include <vector>

#include "query.hpp"
#include "util/compiler_attribute.hpp"

namespace pisa {

template <typename Cursor, typename Inspect>
class InspectingCursor: public Cursor {
  public:
    using base_cursor_type = Cursor;

    InspectingCursor(Cursor cursor, Inspect& inspect)
        : base_cursor_type(std::move(cursor)), m_inspect(inspect)
    {}

    void PISA_ALWAYSINLINE next()
    {
        m_inspect.posting();
        base_cursor_type::next();
    }

    void PISA_ALWAYSINLINE next_geq(uint64_t docid)
    {
        m_inspect.lookup();
        base_cursor_type::next_geq(docid);
    }

  private:
    Inspect& m_inspect;
};

template <typename Cursor, typename Inspect>
[[nodiscard]] auto inspect_cursor(Cursor cursor, Inspect& inspect)
{
    return InspectingCursor<std::decay_t<Cursor>, Inspect>(std::move(cursor), inspect);
}

template <typename Cursor, typename Inspect>
[[nodiscard]] auto inspect_cursors(std::vector<Cursor> cursors, Inspect& inspect)
{
    std::vector<InspectingCursor<std::decay_t<Cursor>, Inspect>> inspecting_cursors;
    inspecting_cursors.reserve(cursors.size());
    for (auto&& cursor: cursors) {
        inspecting_cursors.emplace_back(std::move(cursor), inspect);
    }
    return inspecting_cursors;
}

}  // namespace pisa
