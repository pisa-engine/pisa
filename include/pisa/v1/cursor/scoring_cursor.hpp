#pragma once

#include <utility>

#include <gsl/span>

#include "v1/types.hpp"

namespace pisa::v1 {

template <typename BaseCursor, typename TermScorer>
struct ScoringCursor {
    using Document = decltype(*std::declval<BaseCursor>());
    using Payload = float;
    static_assert(std::is_same_v<typename BaseCursor::Payload, Frequency>);

    explicit constexpr ScoringCursor(BaseCursor base_cursor, TermScorer scorer)
        : m_base_cursor(std::move(base_cursor)), m_scorer(std::move(scorer))
    {
    }

    [[nodiscard]] constexpr auto operator*() const -> Document { return value(); }
    [[nodiscard]] constexpr auto value() const noexcept -> Document
    {
        return m_base_cursor.value();
    }
    [[nodiscard]] constexpr auto payload() const noexcept -> Payload
    {
        return m_scorer(m_base_cursor.value(), m_base_cursor.payload());
    }
    constexpr void advance() { m_base_cursor.advance(); }
    constexpr void advance_to_position(std::size_t pos) { m_base_cursor.advance_to_position(pos); }
    constexpr void advance_to_geq(Document value) { m_base_cursor.advance_to_geq(value); }
    [[nodiscard]] constexpr auto empty() const noexcept -> bool { return m_base_cursor.empty(); }
    [[nodiscard]] constexpr auto position() const noexcept -> std::size_t
    {
        return m_base_cursor.position();
    }
    [[nodiscard]] constexpr auto size() const -> std::size_t { return m_base_cursor.size(); }
    [[nodiscard]] constexpr auto sentinel() const -> Document { return m_base_cursor.sentinel(); }

   private:
    BaseCursor m_base_cursor;
    TermScorer m_scorer;
};

} // namespace pisa::v1
