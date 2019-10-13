#include <iostream>

#include <algorithm>
#include <functional>
#include <numeric>

#include <gsl/span>

#include "util/likely.hpp"

namespace pisa {

/// Transforms a list of cursors into one cursor by lazily merging them together
/// into an intersection.
template <typename CursorContainer, typename Payload, typename AccumulateFn>
struct CursorIntersection {
    using Cursor = typename CursorContainer::value_type;
    using iterator_category =
        typename std::iterator_traits<typename CursorContainer::iterator>::iterator_category;
    static_assert(std::is_base_of<std::random_access_iterator_tag, iterator_category>(),
                  "cursors must be stored in a random access container");
    constexpr CursorIntersection(CursorContainer cursors,
                                 std::size_t max_docid,
                                 Payload init,
                                 AccumulateFn accumulate)
        : m_unordered_cursors(std::move(cursors)),
          m_init(init),
          m_accumulate(std::move(accumulate)),
          m_cursor_mapping(m_unordered_cursors.size()),
          m_size(std::nullopt),
          m_max_docid(max_docid)
    {
        Expects(not m_unordered_cursors.empty());
        std::iota(m_cursor_mapping.begin(), m_cursor_mapping.end(), 0);
        auto order = [&](auto lhs, auto rhs) {
            return m_unordered_cursors[lhs].size() < m_unordered_cursors[rhs].size();
        };
        std::sort(m_cursor_mapping.begin(), m_cursor_mapping.end(), order);
        std::transform(m_cursor_mapping.begin(),
                       m_cursor_mapping.end(),
                       std::back_inserter(m_cursors),
                       [&](auto idx) { return std::ref(m_unordered_cursors[idx]); });
        m_candidate = m_cursors[0].get().docid();
        next();
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
    [[nodiscard]] constexpr auto sentinel() const noexcept -> std::uint32_t { return m_max_docid; }
    [[nodiscard]] constexpr auto payload() const noexcept -> Payload const &
    {
        return m_current_payload;
    }
    constexpr void next()
    {
        while (PISA_LIKELY(m_candidate < m_max_docid)) {
            for (; m_next_cursor < m_cursors.size(); ++m_next_cursor) {
                Cursor &cursor = m_cursors[m_next_cursor];
                cursor.next_geq(m_candidate);
                if (cursor.docid() != m_candidate) {
                    m_candidate = cursor.docid();
                    m_next_cursor = 0;
                    break;
                }
            }
            if (m_next_cursor == m_cursors.size()) {
                m_current_payload = m_init;
                for (auto idx = 0; idx < m_cursors.size(); ++idx) {
                    m_current_payload = m_accumulate(
                        m_current_payload, m_cursors[idx].get(), m_cursor_mapping[idx]);
                }
                m_cursors[0].get().next();
                m_current_docid = std::exchange(m_candidate, m_cursors[0].get().docid());
                m_next_cursor = 1;
                return;
            }
        }
        m_current_docid = m_max_docid;
        m_current_payload = m_init;
        return;
    }

   private:
    CursorContainer m_unordered_cursors;
    Payload m_init;
    AccumulateFn m_accumulate;
    std::vector<std::size_t> m_cursor_mapping;
    std::optional<std::size_t> m_size;
    std::uint32_t m_max_docid;

    std::vector<std::reference_wrapper<Cursor>> m_cursors;
    std::uint32_t m_current_docid{};
    Payload m_current_payload{};
    std::uint32_t m_candidate{};
    std::uint32_t m_next_cursor = 1;
};

} // namespace pisa
