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
        : base_cursor_type(std::move(cursor)), m_inspect(std::ref(inspect))
    {}

    void PISA_ALWAYSINLINE next()
    {
        m_inspect.get().posting();
        base_cursor_type::next();
    }

    void PISA_ALWAYSINLINE next_geq(uint64_t docid)
    {
        if (this->docid() < docid) {
            m_inspect.get().lookup();
            base_cursor_type::next_geq(docid);
        }
    }

  private:
    std::reference_wrapper<Inspect> m_inspect;
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

template <typename Cursor, typename Inspect>
[[nodiscard]] auto inspect_cursor(Cursor cursor, [[maybe_unused]] Inspect* inspect)
{
    if constexpr (std::is_void_v<Inspect>) {
        return cursor;
    } else {
        return InspectingCursor<std::decay_t<Cursor>, Inspect>(std::move(cursor), *inspect);
    }
}

template <typename Cursor, typename Inspect>
[[nodiscard]] auto inspect_cursors(std::vector<Cursor> cursors, [[maybe_unused]] Inspect* inspect)
{
    if constexpr (std::is_void_v<Inspect>) {
        return cursors;
    } else {
        std::vector<InspectingCursor<std::decay_t<Cursor>, Inspect>> inspecting_cursors;
        inspecting_cursors.reserve(cursors.size());
        for (auto&& cursor: cursors) {
            inspecting_cursors.emplace_back(std::move(cursor), *inspect);
        }
        return inspecting_cursors;
    }
}

}  // namespace pisa
