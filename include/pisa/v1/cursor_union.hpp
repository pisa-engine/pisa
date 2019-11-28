#pragma once

#include <iostream>
#include <iterator>
#include <numeric>
#include <optional>

#include <gsl/span>

#include "util/likely.hpp"
#include "v1/algorithm.hpp"

namespace pisa::v1 {

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
    static_assert(std::is_base_of<std::random_access_iterator_tag, iterator_category>(),
                  "cursors must be stored in a random access container");
    using Value = std::decay_t<decltype(*std::declval<Cursor>())>;

    constexpr CursorUnion(CursorContainer cursors, Payload init, AccumulateFn accumulate)
        : m_cursors(std::move(cursors)),
          m_init(std::move(init)),
          m_accumulate(std::move(accumulate)),
          m_size(std::nullopt)
    {
        m_current_payload = m_init;
        if (m_cursors.empty()) {
            m_current_value = std::numeric_limits<Value>::max();
        } else {
            m_next_docid = min_value(m_cursors);
            m_sentinel = min_sentinel(m_cursors);
            advance();
        }
    }

    [[nodiscard]] constexpr auto size() const noexcept -> std::size_t
    {
        if (!m_size) {
            m_size = std::accumulate(m_cursors.begin(),
                                     m_cursors.end(),
                                     std::size_t(0),
                                     [](auto acc, auto const& elem) { return acc + elem.size(); });
        }
        return *m_size;
    }
    [[nodiscard]] constexpr auto operator*() const noexcept -> Value { return m_current_value; }
    [[nodiscard]] constexpr auto value() const noexcept -> Value { return m_current_value; }
    [[nodiscard]] constexpr auto payload() const noexcept -> Payload const&
    {
        return m_current_payload;
    }
    [[nodiscard]] constexpr auto sentinel() const noexcept -> std::uint32_t { return m_sentinel; }

    constexpr void advance()
    {
        if (PISA_UNLIKELY(m_next_docid == m_sentinel)) {
            m_current_value = m_sentinel;
            ::pisa::v1::init_payload(m_current_payload, m_init);
        } else {
            ::pisa::v1::init_payload(m_current_payload, m_init);
            m_current_value = m_next_docid;
            m_next_docid = m_sentinel;
            std::size_t cursor_idx = 0;
            for (auto& cursor : m_cursors) {
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

/// Transforms a list of cursors into one cursor by lazily merging them together.
//template <typename CursorContainer, typename Payload>
//struct CursorFlatUnion {
//    using Cursor = typename CursorContainer::value_type;
//    using iterator_category =
//        typename std::iterator_traits<typename CursorContainer::iterator>::iterator_category;
//    static_assert(std::is_base_of<std::random_access_iterator_tag, iterator_category>(),
//                  "cursors must be stored in a random access container");
//    using Value = std::decay_t<decltype(*std::declval<Cursor>())>;
//
//    explicit constexpr CursorFlatUnion(CursorContainer cursors) : m_cursors(std::move(cursors))
//    {
//        m_current_payload = m_init;
//        if (m_cursors.empty()) {
//            m_current_value = std::numeric_limits<Value>::max();
//        } else {
//            m_next_docid = min_value(m_cursors);
//            m_sentinel = min_sentinel(m_cursors);
//            advance();
//        }
//    }
//
//    [[nodiscard]] constexpr auto operator*() const noexcept -> Value { return m_current_value; }
//    [[nodiscard]] constexpr auto value() const noexcept -> Value { return m_current_value; }
//    [[nodiscard]] constexpr auto payload() const noexcept -> Payload const&
//    {
//        return m_current_payload;
//    }
//    [[nodiscard]] constexpr auto sentinel() const noexcept -> std::uint32_t { return m_sentinel; }
//
//    constexpr void advance()
//    {
//        //if (PISA_UNLIKELY(m_next_docid == m_sentinel)) {
//        //    m_current_value = m_sentinel;
//        //} else {
//        //    m_current_value = m_next_docid;
//        //    m_next_docid = m_sentinel;
//        //    std::size_t cursor_idx = 0;
//        //    for (auto& cursor : m_cursors) {
//        //        if (cursor.value() == m_current_value) {
//        //            m_current_payload = m_accumulate(m_current_payload, cursor, cursor_idx);
//        //            cursor.advance();
//        //        }
//        //        if (cursor.value() < m_next_docid) {
//        //            m_next_docid = cursor.value();
//        //        }
//        //        ++cursor_idx;
//        //    }
//        //}
//    }
//
//    constexpr void advance_to_position(std::size_t pos); // TODO(michal)
//    constexpr void advance_to_geq(Value value); // TODO(michal)
//    [[nodiscard]] constexpr auto position() const noexcept -> std::size_t; // TODO(michal)
//
//    [[nodiscard]] constexpr auto empty() const noexcept -> bool
//    {
//        return m_current_value >= sentinel();
//    }
//
//   private:
//    CursorContainer m_cursors;
//
//    std::size_t m_cursor_idx = 0;
//    Value m_current_value{};
//    Value m_sentinel{};
//    Payload m_current_payload{};
//    std::uint32_t m_next_docid{};
//};

/// Transforms a list of cursors into one cursor by lazily merging them together.
template <typename Payload, typename CursorsTuple, typename... AccumulateFn>
struct VariadicCursorUnion {
    using Value = std::decay_t<decltype(*std::get<0>(std::declval<CursorsTuple>()))>;

    constexpr VariadicCursorUnion(Payload init,
                                  CursorsTuple cursors,
                                  std::tuple<AccumulateFn...> accumulate)
        : m_cursors(std::move(cursors)),
          m_init(std::move(init)),
          m_accumulate(std::move(accumulate)),
          m_size(std::nullopt)
    {
        m_next_docid = std::numeric_limits<Value>::max();
        m_sentinel = std::numeric_limits<Value>::max();
        for_each_cursor([&](auto&& cursor, [[maybe_unused]] auto&& fn) {
            if (cursor.value() < m_next_docid) {
                m_next_docid = cursor.value();
            }
        });
        for_each_cursor([&](auto&& cursor, [[maybe_unused]] auto&& fn) {
            if (cursor.sentinel() < m_next_docid) {
                m_next_docid = cursor.sentinel();
            }
        });
        advance();
    }

    template <typename Fn>
    void for_each_cursor(Fn&& fn)
    {
        std::apply(
            [&](auto&&... cursor) {
                std::apply([&](auto&&... accumulate) { (fn(cursor, accumulate), ...); },
                           m_accumulate);
            },
            m_cursors);
    }

    [[nodiscard]] constexpr auto operator*() const noexcept -> Value { return m_current_value; }
    [[nodiscard]] constexpr auto value() const noexcept -> Value { return m_current_value; }
    [[nodiscard]] constexpr auto payload() const noexcept -> Payload const&
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
            for_each_cursor([&](auto&& cursor, auto&& accumulate) {
                if (cursor.value() == m_current_value) {
                    m_current_payload = accumulate(m_current_payload, cursor, cursor_idx);
                    cursor.advance();
                }
                if (cursor.value() < m_next_docid) {
                    m_next_docid = cursor.value();
                }
                ++cursor_idx;
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

template <typename Payload, typename... Cursors, typename... AccumulateFn>
[[nodiscard]] constexpr inline auto variadic_union_merge(Payload init,
                                                         std::tuple<Cursors...> cursors,
                                                         std::tuple<AccumulateFn...> accumulate)
{
    return VariadicCursorUnion<Payload, std::tuple<Cursors...>, AccumulateFn...>(
        std::move(init), std::move(cursors), std::move(accumulate));
}

} // namespace pisa::v1
