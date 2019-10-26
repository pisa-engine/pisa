#pragma once

#include <iostream>
#include <iterator>
#include <numeric>

#include <gsl/span>

#include "util/likely.hpp"

namespace pisa {

/// Transforms a list of cursors into one cursor by lazily merging them together.
template <typename CursorContainer, typename Payload, typename AccumulateFn>
struct CursorUnion {
    using Cursor = typename CursorContainer::value_type;
    using iterator_category =
        typename std::iterator_traits<typename CursorContainer::iterator>::iterator_category;
    static_assert(std::is_base_of<std::random_access_iterator_tag, iterator_category>(),
                  "cursors must be stored in a random access container");
    constexpr CursorUnion(CursorContainer cursors,
                          std::size_t max_docid,
                          Payload init,
                          AccumulateFn accumulate)
        : m_cursors(std::move(cursors)),
          m_init(init),
          m_accumulate(std::move(accumulate)),
          m_size(std::nullopt),
          m_max_docid(max_docid)
    {
        Expects(not m_cursors.empty());
        auto order = [](auto const &lhs, auto const &rhs) { return lhs.docid() < rhs.docid(); };
        m_next_docid = [&]() {
            auto pos = std::min_element(m_cursors.begin(), m_cursors.end(), order);
            return pos->docid();
        }();
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
    [[nodiscard]] constexpr auto payload() const noexcept -> Payload const &
    {
        return m_current_payload;
    }
    [[nodiscard]] constexpr auto sentinel() const noexcept -> std::uint32_t { return m_max_docid; }
    constexpr void next()
    {
        if (PISA_UNLIKELY(m_next_docid == m_max_docid)) {
            m_current_docid = m_max_docid;
            m_current_payload = m_init;
        } else {
            m_current_payload = m_init;
            m_current_docid = m_next_docid;
            m_next_docid = m_max_docid;
            std::size_t cursor_idx = 0;
            for (auto &cursor : m_cursors) {
                if (cursor.docid() == m_current_docid) {
                    m_current_payload = m_accumulate(m_current_payload, cursor, cursor_idx);
                    cursor.next();
                }
                if (cursor.docid() < m_next_docid) {
                    m_next_docid = cursor.docid();
                }
                ++cursor_idx;
            }
        }
    }

   private:
    CursorContainer m_cursors;
    Payload m_init;
    AccumulateFn m_accumulate;
    std::optional<std::size_t> m_size;
    std::uint32_t m_max_docid;

    std::uint32_t m_current_docid = 0;
    Payload m_current_payload;
    std::uint32_t m_next_docid;
};

} // namespace pisa
