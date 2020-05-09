#pragma once

#include <iostream>
#include <iterator>
#include <numeric>
#include <optional>

#include <gsl/span>

#include "util/likely.hpp"

namespace pisa {

template <typename T>
void init_payload(T& payload, T const& initial_value)
{
    payload = initial_value;
}

template <>
inline void init_payload(std::vector<float>& payload, std::vector<float> const& initial_value)
{
    std::copy(initial_value.begin(), initial_value.end(), payload.begin());
}

/// Transforms a list of cursors into one cursor by lazily merging them together.
template <typename CursorContainer, typename Payload, typename AccumulateFn>
struct CursorUnion {
    using Cursor = typename CursorContainer::value_type;
    using iterator_category =
        typename std::iterator_traits<typename CursorContainer::iterator>::iterator_category;
    static_assert(
        std::is_base_of<std::random_access_iterator_tag, iterator_category>(),
        "cursors must be stored in a random access container");
    using value_type = std::uint32_t;

    constexpr CursorUnion(
        CursorContainer cursors,
        Payload init,
        AccumulateFn accumulate,
        std::optional<value_type> sentinel = std::nullopt)
        : m_cursors(std::move(cursors)), m_init(std::move(init)), m_accumulate(std::move(accumulate))
    {
        m_current_payload = m_init;
        if (m_cursors.empty()) {
            m_current_value = std::numeric_limits<value_type>::max();
        } else {
            m_next_docid =
                std::min_element(m_cursors.begin(), m_cursors.end(), [](auto const& lhs, auto const& rhs) {
                    return lhs.docid() < rhs.docid();
                })->docid();
            if (sentinel) {
                m_sentinel = *sentinel;
            } else {
                m_sentinel = std::max_element(
                                 m_cursors.begin(),
                                 m_cursors.end(),
                                 [](auto const& lhs, auto const& rhs) {
                                     return lhs.universe() < rhs.universe();
                                 })
                                 ->universe();
            }
            next();
        }
    }

    [[nodiscard]] constexpr auto docid() const noexcept -> value_type { return m_current_value; }
    [[nodiscard]] constexpr auto payload() const noexcept -> Payload const&
    {
        return m_current_payload;
    }
    [[nodiscard]] constexpr auto sentinel() const noexcept -> std::uint32_t { return m_sentinel; }

    constexpr void next()
    {
        if (PISA_UNLIKELY(m_next_docid == m_sentinel)) {
            m_current_value = m_sentinel;
            ::pisa::init_payload(m_current_payload, m_init);
        } else {
            ::pisa::init_payload(m_current_payload, m_init);
            m_current_value = m_next_docid;
            m_next_docid = m_sentinel;
            for (auto& cursor: m_cursors) {
                if (cursor.docid() == m_current_value) {
                    m_current_payload = m_accumulate(m_current_payload, cursor);
                    cursor.next();
                }
                if (auto value = cursor.docid(); value < m_next_docid) {
                    m_next_docid = value;
                }
            }
        }
    }

    [[nodiscard]] constexpr auto empty() const noexcept -> bool
    {
        return m_current_value >= sentinel();
    }

  private:
    CursorContainer m_cursors;
    Payload m_init;
    AccumulateFn m_accumulate;

    value_type m_current_value{};
    value_type m_sentinel{};
    Payload m_current_payload{};
    std::uint32_t m_next_docid{};
};

/// Transforms a list of cursors into one cursor by lazily merging them together.
template <typename Payload, typename CursorsTuple, typename... AccumulateFn>
struct VariadicCursorUnion {
    using value_type = std::decay_t<decltype(*std::get<0>(std::declval<CursorsTuple>()))>;

    constexpr VariadicCursorUnion(
        Payload init, CursorsTuple cursors, std::tuple<AccumulateFn...> accumulate)
        : m_cursors(std::move(cursors)),
          m_init(std::move(init)),
          m_accumulate(std::move(accumulate)),
          m_size(std::nullopt)
    {
        m_next_docid = std::numeric_limits<value_type>::max();
        m_sentinel = std::numeric_limits<value_type>::min();
        for_each_cursor([&](auto&& cursor, [[maybe_unused]] auto&& fn) {
            if (cursor.value() < m_next_docid) {
                m_next_docid = cursor.docid();
            }
        });
        for_each_cursor([&](auto&& cursor, [[maybe_unused]] auto&& fn) {
            if (cursor.sentinel() > m_sentinel) {
                m_sentinel = cursor.universe();
            }
        });
        next();
    }

    template <typename Fn>
    void for_each_cursor(Fn&& fn)
    {
        std::apply(
            [&](auto&&... cursor) {
                std::apply(
                    [&](auto&&... accumulate) { (fn(cursor, accumulate), ...); }, m_accumulate);
            },
            m_cursors);
    }

    [[nodiscard]] constexpr auto docid() const noexcept -> value_type { return m_current_value; }
    [[nodiscard]] constexpr auto payload() const noexcept -> Payload const&
    {
        return m_current_payload;
    }
    [[nodiscard]] constexpr auto sentinel() const noexcept -> std::uint32_t { return m_sentinel; }

    constexpr void next()
    {
        if (PISA_UNLIKELY(m_next_docid == m_sentinel)) {
            m_current_value = m_sentinel;
            m_current_payload = m_init;
        } else {
            m_current_payload = m_init;
            m_current_value = m_next_docid;
            m_next_docid = m_sentinel;
            for_each_cursor([&](auto&& cursor, auto&& accumulate) {
                if (cursor.docid() == m_current_value) {
                    m_current_payload = accumulate(m_current_payload, cursor);
                    cursor.next();
                }
                if (cursor.docid() < m_next_docid) {
                    m_next_docid = cursor.value();
                }
            });
        }
    }

    [[nodiscard]] constexpr auto empty() const noexcept -> bool
    {
        return m_current_value >= sentinel();
    }

  private:
    CursorsTuple m_cursors;
    Payload m_init;
    std::tuple<AccumulateFn...> m_accumulate;
    std::optional<std::size_t> m_size;

    value_type m_current_value{};
    value_type m_sentinel{};
    Payload m_current_payload{};
    std::uint32_t m_next_docid{};
};

template <typename CursorContainer, typename Payload, typename AccumulateFn>
[[nodiscard]] constexpr inline auto union_merge(
    CursorContainer cursors,
    Payload init,
    AccumulateFn accumulate,
    std::optional<std::uint32_t> sentinel = std::nullopt)
{
    return CursorUnion<CursorContainer, Payload, AccumulateFn>(
        std::move(cursors), std::move(init), std::move(accumulate), sentinel);
}

template <typename Payload, typename... Cursors, typename... AccumulateFn>
[[nodiscard]] constexpr inline auto variadic_union_merge(
    Payload init, std::tuple<Cursors...> cursors, std::tuple<AccumulateFn...> accumulate)
{
    return VariadicCursorUnion<Payload, std::tuple<Cursors...>, AccumulateFn...>(
        std::move(init), std::move(cursors), std::move(accumulate));
}

}  // namespace pisa
