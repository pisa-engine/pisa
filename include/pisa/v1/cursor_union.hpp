#pragma once

#include <iostream>
#include <iterator>
#include <numeric>

#include <gsl/span>

#include "util/likely.hpp"

namespace pisa::v1 {

/// Transforms a list of cursors into one cursor by lazily merging them together.
template <typename CursorContainer, typename Payload, typename AccumulateFn>
struct CursorUnion {
    using Cursor = typename CursorContainer::value_type;
    using iterator_category =
        typename std::iterator_traits<typename CursorContainer::iterator>::iterator_category;
    static_assert(std::is_base_of<std::random_access_iterator_tag, iterator_category>(),
                  "cursors must be stored in a random access container");
    using Value = std::decay_t<decltype(*std::declval<Cursor>())>;

    constexpr CursorUnion(CursorContainer cursors, Payload init, AccumulateFn accumulate)
        : m_cursors(std::move(cursors)),
          m_init(std::move(init)),
          m_accumulate(std::move(accumulate)),
          m_size(std::nullopt)
    {
        Expects(not m_cursors.empty());
        auto order = [](auto const &lhs, auto const &rhs) { return lhs.value() < rhs.value(); };
        m_next_docid = [&]() {
            auto pos = std::min_element(m_cursors.begin(), m_cursors.end(), order);
            return pos->value();
        }();
        m_sentinel = std::min_element(m_cursors.begin(),
                                      m_cursors.end(),
                                      [](auto const &lhs, auto const &rhs) {
                                          return lhs.sentinel() < rhs.sentinel();
                                      })
                         ->sentinel();
        advance();
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
    [[nodiscard]] constexpr auto operator*() const noexcept -> Value { return m_current_value; }
    [[nodiscard]] constexpr auto value() const noexcept -> Value { return m_current_value; }
    [[nodiscard]] constexpr auto payload() const noexcept -> Payload const &
    {
        return m_current_payload;
    }
    [[nodiscard]] constexpr auto sentinel() const noexcept -> std::uint32_t { return m_sentinel; }

    constexpr void advance()
    {
        if (PISA_UNLIKELY(m_next_docid == m_sentinel)) {
            m_current_value = m_sentinel;
            m_current_payload = m_init;
        } else {
            m_current_payload = m_init;
            m_current_value = m_next_docid;
            m_next_docid = m_sentinel;
            std::size_t cursor_idx = 0;
            for (auto &cursor : m_cursors) {
                if (cursor.value() == m_current_value) {
                    m_current_payload = m_accumulate(m_current_payload, cursor, cursor_idx);
                    cursor.advance();
                }
                if (cursor.value() < m_next_docid) {
                    m_next_docid = cursor.value();
                }
                ++cursor_idx;
            }
        }
    }

    constexpr void advance_to_position(std::size_t pos); // TODO(michal)
    constexpr void advance_to_geq(Value value); // TODO(michal)
    [[nodiscard]] constexpr auto position() const noexcept -> std::size_t; // TODO(michal)

    [[nodiscard]] constexpr auto empty() const noexcept -> bool
    {
        return m_current_value >= sentinel();
    }

   private:
    CursorContainer m_cursors;
    Payload m_init;
    AccumulateFn m_accumulate;
    std::optional<std::size_t> m_size;

    Value m_current_value{};
    Value m_sentinel{};
    Payload m_current_payload{};
    std::uint32_t m_next_docid{};
};

template <typename CursorContainer, typename Payload, typename AccumulateFn>
[[nodiscard]] constexpr inline auto union_merge(CursorContainer cursors,
                                                Payload init,
                                                AccumulateFn accumulate)
{
    return CursorUnion<CursorContainer, Payload, AccumulateFn>(
        std::move(cursors), std::move(init), std::move(accumulate));
}

} // namespace pisa::v1
