#pragma once

#include <algorithm>
#include <functional>
#include <iostream>
#include <numeric>

#include <gsl/gsl_assert>

#include "cursor/cursor.hpp"
#include "util/likely.hpp"

namespace pisa {

/// Transforms a list of cursors into one cursor by lazily merging them together
/// into an intersection.
template <typename CursorContainer, typename Payload, typename AccumulateFn>
struct CursorIntersection: CursorJoin<typename CursorContainer::value_type, Payload, AccumulateFn> {
    using iterator_category =
        typename std::iterator_traits<typename CursorContainer::iterator>::iterator_category;
    static_assert(
        std::is_base_of<std::random_access_iterator_tag, iterator_category>(),
        "cursors must be stored in a random access container");

    using cursor_join_type = CursorJoin<typename CursorContainer::value_type, Payload, AccumulateFn>;
    using cursor_type = typename CursorContainer::value_type;
    using value_type = std::uint32_t;
    using payload_type = Payload;

    constexpr CursorIntersection(
        CursorContainer cursors,
        Payload init,
        AccumulateFn accumulate,
        std::optional<value_type> sentinel = std::nullopt)
        : cursor_join_type(std::move(init), std::move(accumulate)),
          m_unordered_cursors(std::move(cursors)),
          m_cursor_mapping(m_unordered_cursors.size())
    {
        Expects(not m_unordered_cursors.empty());
        std::iota(m_cursor_mapping.begin(), m_cursor_mapping.end(), 0);
        auto order = [&](auto lhs, auto rhs) {
            return m_unordered_cursors[lhs].size() < m_unordered_cursors[rhs].size();
        };
        std::sort(m_cursor_mapping.begin(), m_cursor_mapping.end(), order);
        std::transform(
            m_cursor_mapping.begin(),
            m_cursor_mapping.end(),
            std::back_inserter(m_cursors),
            [&](auto idx) { return std::ref(m_unordered_cursors[idx]); });
        if (sentinel) {
            this->set_sentinel(*sentinel);
        } else {
            auto min_sentinel =
                std::min_element(
                    m_unordered_cursors.begin(),
                    m_unordered_cursors.end(),
                    [](auto const& lhs, auto const& rhs) { return lhs.universe() < rhs.universe(); })
                    ->universe();
            this->set_sentinel(min_sentinel);
        }
        m_candidate = m_cursors[0].get().docid();
        next();
    }

    constexpr void next()
    {
        while (PISA_LIKELY(m_candidate < this->sentinel())) {
            for (; m_next_cursor < m_cursors.size(); ++m_next_cursor) {
                cursor_type& cursor = m_cursors[m_next_cursor].get();
                cursor.next_geq(m_candidate);
                if (cursor.docid() != m_candidate) {
                    m_candidate = cursor.docid();
                    m_next_cursor = 0;
                    break;
                }
            }
            if (m_next_cursor == m_cursors.size()) {
                this->init_payload();
                for (auto idx = 0; idx < m_cursors.size(); ++idx) {
                    this->accumulate(m_cursors[idx].get());
                }
                m_cursors[0].get().next();
                this->set_current_value(m_candidate);
                m_candidate = m_cursors[0].get().docid();
                m_next_cursor = 1;
                return;
            }
        }
        this->set_current_value(this->sentinel());
        this->init_payload();
    }

  private:
    CursorContainer m_unordered_cursors;
    std::vector<std::size_t> m_cursor_mapping;

    std::vector<std::reference_wrapper<cursor_type>> m_cursors;
    value_type m_candidate{};
    std::uint32_t m_next_cursor = 1;
};

template <typename CursorContainer, typename Payload, typename AccumulateFn>
[[nodiscard]] constexpr inline auto intersect(
    CursorContainer cursors,
    Payload init,
    AccumulateFn accumulate,
    std::optional<std::uint32_t> sentinel = std::nullopt)
{
    return CursorIntersection<CursorContainer, Payload, AccumulateFn>(
        std::move(cursors), std::move(init), std::move(accumulate), sentinel);
}

template <typename Cursor, typename Payload, typename AccumulateFn>
[[nodiscard]] constexpr inline auto
intersect(std::initializer_list<Cursor> cursors, Payload init, AccumulateFn accumulate)
{
    std::vector<Cursor> cursor_container(cursors);
    return CursorIntersection<std::vector<Cursor>, Payload, AccumulateFn>(
        std::move(cursor_container), std::move(init), std::move(accumulate));
}

}  // namespace pisa
