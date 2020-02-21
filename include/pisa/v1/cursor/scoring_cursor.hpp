#pragma once

#include <utility>

#include <gsl/span>

#include "v1/cursor_traits.hpp"
#include "v1/types.hpp"

namespace pisa::v1 {

struct ScoringCursorTag {
};
struct MaxScoreCursorTag {
};
struct BlockMaxScoreCursorTag {
};

template <typename BaseCursor, typename TermScorer>
struct ScoringCursor {
    using Document = decltype(*std::declval<BaseCursor>());
    using Payload = decltype((std::declval<TermScorer>())(std::declval<Document>(),
                                                          std::declval<BaseCursor>().payload()));
    using Tag = ScoringCursorTag;

    explicit constexpr ScoringCursor(BaseCursor base_cursor, TermScorer scorer)
        : m_base_cursor(std::move(base_cursor)), m_scorer(std::move(scorer))
    {
    }
    constexpr ScoringCursor(ScoringCursor const&) = default;
    constexpr ScoringCursor(ScoringCursor&&) noexcept = default;
    constexpr ScoringCursor& operator=(ScoringCursor const&) = default;
    constexpr ScoringCursor& operator=(ScoringCursor&&) noexcept = default;
    ~ScoringCursor() = default;

    [[nodiscard]] constexpr auto operator*() const -> Document { return value(); }
    [[nodiscard]] constexpr auto value() const noexcept -> Document
    {
        return m_base_cursor.value();
    }
    [[nodiscard]] constexpr auto payload() noexcept
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

template <typename BaseCursor, typename ScoreT>
struct MaxScoreCursor {
    using Document = decltype(*std::declval<BaseCursor>());
    using Payload = decltype(std::declval<BaseCursor>().payload());
    using Tag = MaxScoreCursorTag;

    constexpr MaxScoreCursor(BaseCursor base_cursor, ScoreT max_score)
        : m_base_cursor(std::move(base_cursor)), m_max_score(max_score)
    {
    }
    constexpr MaxScoreCursor(MaxScoreCursor const&) = default;
    constexpr MaxScoreCursor(MaxScoreCursor&&) noexcept = default;
    constexpr MaxScoreCursor& operator=(MaxScoreCursor const&) = default;
    constexpr MaxScoreCursor& operator=(MaxScoreCursor&&) noexcept = default;
    ~MaxScoreCursor() = default;

    [[nodiscard]] constexpr auto operator*() const -> Document { return m_base_cursor.value(); }
    [[nodiscard]] constexpr auto value() const noexcept -> Document
    {
        return m_base_cursor.value();
    }
    [[nodiscard]] constexpr auto payload() noexcept { return m_base_cursor.payload(); }
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
    [[nodiscard]] constexpr auto max_score() const -> ScoreT { return m_max_score; }

   private:
    BaseCursor m_base_cursor;
    float m_max_score;
};

template <typename BaseScoredCursor, typename BlockMaxCursor, typename ScoreT>
struct BlockMaxScoreCursor {
    using Document = decltype(*std::declval<BaseScoredCursor>());
    using Payload = decltype(std::declval<BaseScoredCursor>().payload());
    using Tag = BlockMaxScoreCursorTag;

    constexpr BlockMaxScoreCursor(BaseScoredCursor base_cursor,
                                  BlockMaxCursor block_max_cursor,
                                  ScoreT max_score)
        : m_base_cursor(std::move(base_cursor)),
          m_block_max_cursor(block_max_cursor),
          m_max_score(max_score)
    {
    }
    constexpr BlockMaxScoreCursor(BlockMaxScoreCursor const&) = default;
    constexpr BlockMaxScoreCursor(BlockMaxScoreCursor&&) noexcept = default;
    constexpr BlockMaxScoreCursor& operator=(BlockMaxScoreCursor const&) = default;
    constexpr BlockMaxScoreCursor& operator=(BlockMaxScoreCursor&&) noexcept = default;
    ~BlockMaxScoreCursor() = default;

    [[nodiscard]] constexpr auto operator*() const -> Document { return m_base_cursor.value(); }
    [[nodiscard]] constexpr auto value() const noexcept -> Document
    {
        return m_base_cursor.value();
    }
    [[nodiscard]] constexpr auto payload() noexcept { return m_base_cursor.payload(); }
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
    [[nodiscard]] constexpr auto max_score() const -> float { return m_max_score; }
    [[nodiscard]] constexpr auto block_max_docid() { return m_block_max_cursor.value(); }
    [[nodiscard]] constexpr auto block_max_score() { return m_block_max_cursor.payload(); }
    [[nodiscard]] constexpr auto block_max_score(DocId docid)
    {
        m_block_max_cursor.advance_to_geq(docid);
        return m_block_max_cursor.payload();
    }

   private:
    BaseScoredCursor m_base_cursor;
    BlockMaxCursor m_block_max_cursor;
    float m_max_score;
};

template <typename BaseScoredCursor, typename BlockMaxCursor, typename ScoreT>
[[nodiscard]] auto block_max_score_cursor(BaseScoredCursor base_cursor,
                                          BlockMaxCursor block_max_cursor,
                                          ScoreT max_score)
{
    return BlockMaxScoreCursor<BaseScoredCursor, BlockMaxCursor, ScoreT>(
        std::move(base_cursor), std::move(block_max_cursor), max_score);
}

template <typename BaseCursor, typename TermScorer>
struct CursorTraits<ScoringCursor<BaseCursor, TermScorer>> {
    using Value = typename CursorTraits<BaseCursor>::Value;
};

template <typename BaseCursor, typename ScoreT>
struct CursorTraits<MaxScoreCursor<BaseCursor, ScoreT>> {
    using Value = typename CursorTraits<BaseCursor>::Value;
};

template <typename BaseScoredCursor, typename BlockMaxCursor, typename ScoreT>
struct CursorTraits<BlockMaxScoreCursor<BaseScoredCursor, BlockMaxCursor, ScoreT>> {
    using Value = typename CursorTraits<BaseScoredCursor>::Value;
};

} // namespace pisa::v1
