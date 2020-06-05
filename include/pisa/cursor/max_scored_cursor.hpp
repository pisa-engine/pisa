#pragma once

#include <vector>

#include "cursor/scored_cursor.hpp"
#include "query.hpp"
#include "wand_data.hpp"

namespace pisa {

template <typename S1, typename S2>
class PairScorer {
  public:
    PairScorer(S1 left_scorer, S2 right_scorer)
        : m_left_scorer(std::move(left_scorer)), m_right_scorer(std::move(right_scorer))
    {}

    [[nodiscard]] auto operator()(TermId term_id, std::array<std::size_t, 2> frequencies)
        -> std::array<float, 2>
    {
        return {
            m_left_scorer(term_id, std::get<0>(frequencies)),
            m_right_scorer(term_id, std::get<1>(frequencies))};
    }

  private:
    S1 m_left_scorer;
    S2 m_right_scorer;
};

template <typename S1, typename S2>
[[nodiscard]] auto make_pair_scorer(S1 left_scorer, S2 right_scorer)
{
    return PairScorer<S1, S2>(std::move(left_scorer), std::move(right_scorer));
}

template <typename Cursor>
class MaxScoredCursor: public ScoredCursor<Cursor> {
  public:
    using base_cursor_type = Cursor;

    MaxScoredCursor(Cursor cursor, TermScorer term_scorer, float query_weight, float max_score)
        : ScoredCursor<Cursor>(std::move(cursor), std::move(term_scorer), query_weight),
          m_max_score(max_score)
    {}
    MaxScoredCursor(MaxScoredCursor const&) = delete;
    MaxScoredCursor(MaxScoredCursor&&) = default;
    MaxScoredCursor& operator=(MaxScoredCursor const&) = delete;
    MaxScoredCursor& operator=(MaxScoredCursor&&) = default;
    ~MaxScoredCursor() = default;

    [[nodiscard]] PISA_ALWAYSINLINE auto max_score() const noexcept -> float { return m_max_score; }

  private:
    float m_max_score;
    float m_query_weight = 1.0;
};

template <typename Cursor>
class PairMaxScoredCursor: public PairScoredCursor<Cursor> {
  public:
    using base_cursor_type = Cursor;

    PairMaxScoredCursor(
        Cursor cursor, TermScorer left_scorer, TermScorer right_scorer, float query_weight, float max_score)
        : PairScoredCursor<Cursor>(
            std::move(cursor), std::move(left_scorer), std::move(right_scorer), query_weight),
          m_max_score(max_score)
    {}
    PairMaxScoredCursor(PairMaxScoredCursor const&) = delete;
    PairMaxScoredCursor(PairMaxScoredCursor&&) = default;
    PairMaxScoredCursor& operator=(PairMaxScoredCursor const&) = delete;
    PairMaxScoredCursor& operator=(PairMaxScoredCursor&&) = default;
    ~PairMaxScoredCursor() = default;

    [[nodiscard]] PISA_ALWAYSINLINE auto max_score() const noexcept -> float { return m_max_score; }

  private:
    float m_max_score;
    float m_query_weight = 1.0;
};

template <typename Index, typename WandType, typename Scorer>
[[nodiscard]] auto make_max_scored_pair_cursor(
    Index const& index,
    WandType const& wdata,
    TermId pair_id,
    Scorer const& scorer,
    TermId left_term,
    TermId right_term)
{
    using cursor_type = PairMaxScoredCursor<typename Index::document_enumerator>;
    auto left_max_weight = wdata.max_term_weight(left_term);
    auto right_max_weight = wdata.max_term_weight(right_term);
    return cursor_type(
        index[pair_id],
        scorer.term_scorer(left_term),
        scorer.term_scorer(right_term),
        1.0,
        left_max_weight + right_max_weight);
}

template <typename Index, typename WandType, typename Scorer>
[[nodiscard]] auto
make_max_scored_cursor(Index const& index, WandType const& wdata, Scorer const& scorer, TermId term_id)
{
    using cursor_type = MaxScoredCursor<typename Index::document_enumerator>;
    auto max_weight = wdata.max_term_weight(term_id);
    return cursor_type(index[term_id], scorer.term_scorer(term_id), 1.0, max_weight);
}

template <typename Index, typename WandType, typename Scorer>
[[nodiscard]] auto make_max_scored_cursors(
    Index const& index, WandType const& wdata, Scorer const& scorer, gsl::span<TermId const> term_ids)
{
    using cursor_type = MaxScoredCursor<typename Index::document_enumerator>;
    std::vector<cursor_type> cursors;
    cursors.reserve(term_ids.size());
    std::transform(term_ids.begin(), term_ids.end(), std::back_inserter(cursors), [&](auto term_id) {
        auto max_weight = wdata.max_term_weight(term_id);
        return cursor_type(index[term_id], scorer.term_scorer(term_id), 1.0, max_weight);
    });
    return cursors;
}

template <typename Index, typename WandType, typename Scorer>
[[nodiscard]] auto make_max_scored_cursors(
    Index const& index, WandType const& wdata, Scorer const& scorer, QueryRequest query)
{
    using cursor_type = MaxScoredCursor<typename Index::document_enumerator>;
    auto term_ids = query.term_ids();
    auto term_weights = query.term_weights();
    std::vector<cursor_type> cursors;
    cursors.reserve(term_ids.size());
    std::transform(
        term_ids.begin(),
        term_ids.end(),
        term_weights.begin(),
        std::back_inserter(cursors),
        [&](auto term_id, auto weight) {
            auto max_weight = weight * wdata.max_term_weight(term_id);
            return cursor_type(index[term_id], scorer.term_scorer(term_id), weight, max_weight);
        });
    return cursors;
}  // namespace pisa

}  // namespace pisa
