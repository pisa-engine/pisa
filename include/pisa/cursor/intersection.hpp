#include <numeric>

#include <gsl/span>

#include "util/likely.hpp"

namespace pisa {

/// Transforms a list of cursors into one cursor by lazily merging them together
/// into an intersection.
template <typename Cursor, typename Payload, typename AccumulateFn>
struct CursorIntersection {
    constexpr CursorIntersection(gsl::span<Cursor> cursors,
                                 std::size_t max_docid,
                                 Payload init,
                                 AccumulateFn accumulate)
        : m_accumulate(std::move(accumulate)),
          m_init(init),
          m_cursors(std::move(cursors)),
          m_size(std::nullopt),
          m_max_docid(max_docid)
    {
        Expects(not cursors.empty());
        /* auto order = [](auto const &lhs, auto const &rhs) { return lhs.docid() < rhs.docid(); }; */
        /* m_next_docid = [&]() { */
        /*     auto pos = std::min_element(cursors.begin(), cursors.end(), order); */
        /*     return pos->docid(); */
        /* }(); */
        /* next(); */
    }

    [[nodiscard]] constexpr auto size() const noexcept -> std::size_t
    {
        if (!m_size) {
            m_size = std::accumulate(m_cursors.begin(),
                                     m_cursors.end(),
                                     std::size_t(0),
                                     [](auto acc, auto const &elem) { return acc + elem.size(); });
        }
        return *m_size;
    }
    [[nodiscard]] constexpr auto docid() const noexcept -> std::uint32_t { return m_current_docid; }
    [[nodiscard]] constexpr auto payload() const noexcept -> Payload { return m_current_payload; }
    constexpr void next()
    {
        /* if (PISA_UNLIKELY(m_next_docid == m_max_docid)) { */
        /*     m_current_docid = m_max_docid; */
        /*     m_current_payload = m_init; */
        /*     return; */
        /* } else { */
        /*     m_current_payload = m_init; */
        /*     m_current_docid = m_next_docid; */
        /*     m_next_docid = m_max_docid; */
        /*     for (auto &cursor : m_cursors) { */
        /*         if (cursor.docid() == m_current_docid) { */
        /*             m_current_payload = m_accumulate(m_current_payload, cursor); */
        /*             cursor.next(); */
        /*         } */
        /*         if (cursor.docid() < m_next_docid) { */
        /*             m_next_docid = cursor.docid(); */
        /*         } */
        /*     } */
        /* } */
    }

   private:
    AccumulateFn m_accumulate;
    Payload m_init;
    gsl::span<Cursor> m_cursors;
    std::optional<std::size_t> m_size;
    std::uint32_t m_max_docid;

    std::uint32_t m_current_docid;
    Payload m_current_payload;
    std::uint32_t m_next_docid;
};

} // namespace pisa
