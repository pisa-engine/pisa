#pragma once

#include <algorithm>
#include <functional>
#include <iostream>
#include <numeric>

#include <gsl/gsl_assert>

#include "util/likely.hpp"

namespace pisa {

/// Transforms a list of cursors into one cursor by lazily merging them together
/// into an intersection.
template <typename CursorContainer, typename Payload, typename AccumulateFn>
struct CursorIntersection {
    using cursor_type = typename CursorContainer::value_type;
    using iterator_category =
        typename std::iterator_traits<typename CursorContainer::iterator>::iterator_category;
    static_assert(
        std::is_base_of<std::random_access_iterator_tag, iterator_category>(),
        "cursors must be stored in a random access container");
    using value_type = std::uint32_t;
    using payload_type = Payload;

    constexpr CursorIntersection(CursorContainer cursors, Payload init, AccumulateFn accumulate)
        : m_unordered_cursors(std::move(cursors)),
          m_init(std::move(init)),
          m_accumulate(std::move(accumulate)),
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
        m_sentinel =
            std::min_element(
                m_unordered_cursors.begin(),
                m_unordered_cursors.end(),
                [](auto const& lhs, auto const& rhs) { return lhs.universe() < rhs.universe(); })
                ->universe();
        m_candidate = m_cursors[0].get().docid();
        next();
    }

    [[nodiscard]] constexpr auto docid() const noexcept -> value_type { return m_current_value; }

    constexpr void next()
    {
        while (PISA_LIKELY(m_candidate < sentinel())) {
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
                m_current_payload = m_init;
                for (auto idx = 0; idx < m_cursors.size(); ++idx) {
                    m_current_payload =
                        m_accumulate(m_current_payload, m_cursors[idx].get(), m_cursor_mapping[idx]);
                }
                m_cursors[0].get().next();
                m_current_value = std::exchange(m_candidate, m_cursors[0].get().docid());
                m_next_cursor = 1;
                return;
            }
        }
        m_current_value = sentinel();
        m_current_payload = m_init;
    }

    [[nodiscard]] constexpr auto payload() const noexcept -> Payload const&
    {
        return m_current_payload;
    }

    [[nodiscard]] constexpr auto empty() const noexcept -> bool
    {
        return m_current_value >= sentinel();
    }

    [[nodiscard]] constexpr auto sentinel() const noexcept -> std::size_t { return m_sentinel; }
    [[nodiscard]] constexpr auto size() const -> std::size_t = delete;

  private:
    CursorContainer m_unordered_cursors;
    Payload m_init;
    AccumulateFn m_accumulate;
    std::vector<std::size_t> m_cursor_mapping;

    std::vector<std::reference_wrapper<cursor_type>> m_cursors;
    value_type m_current_value{};
    value_type m_candidate{};
    value_type m_sentinel{};
    payload_type m_current_payload{};
    std::uint32_t m_next_cursor = 1;
};

template <typename CursorContainer, typename Payload, typename AccumulateFn>
[[nodiscard]] constexpr inline auto
intersect(CursorContainer cursors, Payload init, AccumulateFn accumulate)
{
    return CursorIntersection<CursorContainer, Payload, AccumulateFn>(
        std::move(cursors), std::move(init), std::move(accumulate));
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
