#pragma once

#include <iostream>
#include <iterator>
#include <numeric>
#include <optional>

#include <gsl/span>

#include "cursor/cursor.hpp"
#include "util/compiler_attribute.hpp"
#include "util/likely.hpp"

namespace pisa {

/// Transforms a list of cursors into one cursor by lazily merging them together.
template <typename CursorContainer, typename Payload, typename AccumulateFn>
struct CursorUnion: public CursorJoin<typename CursorContainer::value_type, Payload, AccumulateFn> {
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
        : CursorJoin<typename CursorContainer::value_type, Payload, AccumulateFn>(
            std::move(init), std::move(accumulate)),
          m_cursors(std::move(cursors))
    {
        if (m_cursors.empty()) {
            this->set_current_value(std::numeric_limits<value_type>::max());
        } else {
            m_next_docid =
                std::min_element(m_cursors.begin(), m_cursors.end(), [](auto const& lhs, auto const& rhs) {
                    return lhs.docid() < rhs.docid();
                })->docid();
            if (sentinel) {
                this->set_sentinel(*sentinel);
            } else {
                auto max_sentinel = std::max_element(
                                        m_cursors.begin(),
                                        m_cursors.end(),
                                        [](auto const& lhs, auto const& rhs) {
                                            return lhs.universe() < rhs.universe();
                                        })
                                        ->universe();
                this->set_sentinel(max_sentinel);
            }
            next();
        }
    }

    constexpr PISA_ALWAYSINLINE void next()
    {
        if (PISA_UNLIKELY(m_next_docid == this->sentinel())) {
            this->set_current_value(this->sentinel());
            this->init_payload();
        } else {
            this->init_payload();
            this->set_current_value(m_next_docid);
            m_next_docid = this->sentinel();
            for (auto& cursor: m_cursors) {
                if (cursor.docid() == this->docid()) {
                    this->accumulate(cursor);
                    cursor.next();
                }
                if (auto value = cursor.docid(); value < m_next_docid) {
                    m_next_docid = value;
                }
            }
        }
    }

  private:
    CursorContainer m_cursors;
    std::uint32_t m_next_docid{};
};

/// Transforms a list of cursors into one cursor by lazily merging them together.
template <typename Payload, typename CursorsTuple, typename... AccumulateFn>
struct VariadicCursorUnion {
    using value_type = std::decay_t<decltype(std::get<0>(std::declval<CursorsTuple>()).docid())>;

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
            if (cursor.docid() < m_next_docid) {
                m_next_docid = cursor.docid();
            }
        });
        for_each_cursor([&](auto&& cursor, [[maybe_unused]] auto&& fn) {
            if (cursor.sentinel() > m_sentinel) {
                m_sentinel = cursor.sentinel();
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
    [[nodiscard]] constexpr auto score() const noexcept -> Payload const&
    {
        return m_current_payload;
    }
    [[nodiscard]] constexpr auto payload() const noexcept -> Payload const&
    {
        return m_current_payload;
    }
    [[nodiscard]] constexpr auto sentinel() const noexcept -> std::uint32_t { return m_sentinel; }
    [[nodiscard]] constexpr auto universe() const noexcept -> std::uint32_t { return m_sentinel; }

    constexpr void next()
    {
        if (PISA_UNLIKELY(m_next_docid == m_sentinel)) {
            m_current_value = m_sentinel;
            ::pisa::init_payload(m_current_payload, m_init);
        } else {
            ::pisa::init_payload(m_current_payload, m_init);
            m_current_value = m_next_docid;
            m_next_docid = m_sentinel;
            for_each_cursor([&](auto&& cursor, auto&& accumulate) {
                if (cursor.docid() == m_current_value) {
                    m_current_payload = accumulate(m_current_payload, cursor);
                    cursor.next();
                }
                if (cursor.docid() < m_next_docid) {
                    m_next_docid = cursor.docid();
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

/// Transforms a list of cursors into one cursor by lazily merging them together.
template <typename Payload, typename CursorsTuple, typename... AccumulateFn>
struct GenericCursorUnion {
    using value_type = std::uint32_t;

    constexpr GenericCursorUnion(Payload init, CursorsTuple cursors, std::tuple<AccumulateFn...> accumulate)
        : m_cursors(std::move(cursors)), m_init(std::move(init)), m_accumulate(std::move(accumulate))
    {
        m_next_docid = std::numeric_limits<value_type>::max();
        m_sentinel = std::numeric_limits<value_type>::min();
        for_each_cursor([&](auto&& cursor, [[maybe_unused]] auto&& fn) {
            if (cursor.docid() < m_next_docid) {
                m_next_docid = cursor.docid();
            }
        });
        for_each_cursor([&](auto&& cursor, [[maybe_unused]] auto&& fn) {
            if (cursor.universe() > m_sentinel) {
                m_sentinel = cursor.universe();
            }
        });
        next();
    }

    template <typename Fn>
    PISA_ALWAYSINLINE void for_each_cursor(Fn&& fn)
    {
        auto inner_loop = [&](auto&& inner_cursors, auto&& accumulate) {
            for (auto&& cursor: inner_cursors) {
                fn(std::forward<decltype(cursor)>(cursor),
                   std::forward<decltype(accumulate)>(accumulate));
            }
        };
        std::apply(
            [&](auto&&... inner_cursors) {
                std::apply(
                    [&](auto&&... accumulate) {
                        (inner_loop(
                             std::forward<decltype(inner_cursors)>(inner_cursors),
                             std::forward<decltype(accumulate)>(accumulate)),
                         ...);
                    },
                    m_accumulate);
            },
            m_cursors);
    }

    [[nodiscard]] PISA_ALWAYSINLINE constexpr auto docid() const noexcept -> value_type
    {
        return m_current_value;
    }
    [[nodiscard]] PISA_ALWAYSINLINE constexpr auto score() const noexcept -> Payload const&
    {
        return m_current_payload;
    }
    [[nodiscard]] PISA_ALWAYSINLINE constexpr auto payload() const noexcept -> Payload const&
    {
        return m_current_payload;
    }
    [[nodiscard]] PISA_ALWAYSINLINE constexpr auto sentinel() const noexcept -> std::uint32_t
    {
        return m_sentinel;
    }
    [[nodiscard]] PISA_ALWAYSINLINE constexpr auto universe() const noexcept -> std::uint32_t
    {
        return m_sentinel;
    }

    PISA_ALWAYSINLINE constexpr void next()
    {
        if (PISA_UNLIKELY(m_next_docid == m_sentinel)) {
            m_current_value = m_sentinel;
            ::pisa::init_payload(m_current_payload, m_init);
        } else {
            ::pisa::init_payload(m_current_payload, m_init);
            m_current_value = m_next_docid;
            m_next_docid = m_sentinel;
            for_each_cursor([&](auto&& cursor, auto&& accumulate) {
                if (cursor.docid() == m_current_value) {
                    m_current_payload = accumulate(m_current_payload, cursor);
                    cursor.next();
                }
                if (cursor.docid() < m_next_docid) {
                    m_next_docid = cursor.docid();
                }
            });
        }
    }

    [[nodiscard]] PISA_ALWAYSINLINE constexpr auto empty() const noexcept -> bool
    {
        return m_current_value >= sentinel();
    }

  private:
    CursorsTuple m_cursors;
    Payload m_init;
    std::tuple<AccumulateFn...> m_accumulate;

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

template <typename Payload, typename... Cursors, typename... AccumulateFn>
[[nodiscard]] constexpr inline auto generic_union_merge(
    Payload init, std::tuple<Cursors...> cursors, std::tuple<AccumulateFn...> accumulate)
{
    return GenericCursorUnion<Payload, std::tuple<Cursors...>, AccumulateFn...>(
        std::move(init), std::move(cursors), std::move(accumulate));
}

}  // namespace pisa
