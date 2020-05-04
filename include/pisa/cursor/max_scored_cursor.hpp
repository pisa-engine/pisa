#pragma once

#include <vector>

#include "cursor/scored_cursor.hpp"
#include "query/queries.hpp"
#include "wand_data.hpp"

namespace pisa {

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

template <typename Index, typename WandType, typename Scorer>
[[nodiscard]] auto
make_max_scored_cursors(Index const& index, WandType const& wdata, Scorer const& scorer, Query query)
{
    auto terms = query.terms;
    auto query_term_freqs = query_freqs(terms);

    std::vector<MaxScoredCursor<typename Index::document_enumerator>> cursors;
    cursors.reserve(query_term_freqs.size());
    std::transform(
        query_term_freqs.begin(), query_term_freqs.end(), std::back_inserter(cursors), [&](auto&& term) {
            float query_weight = term.second;
            auto max_weight = query_weight * wdata.max_term_weight(term.first);
            return MaxScoredCursor<typename Index::document_enumerator>(
                index[term.first], scorer.term_scorer(term.first), query_weight, max_weight);
        });
    return cursors;
}

}  // namespace pisa
