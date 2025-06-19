#pragma once

#include <vector>

#include "cursor/scored_cursor.hpp"
#include "query.hpp"
#include "util/compiler_attribute.hpp"

namespace pisa {

template <typename Cursor>
    requires(concepts::FrequencyPostingCursor<Cursor> && concepts::SortedPostingCursor<Cursor>)
class MaxScoredCursor: public ScoredCursor<Cursor> {
  public:
    using base_cursor_type = Cursor;

    MaxScoredCursor(Cursor cursor, TermScorer term_scorer, float weight, float max_score)
        : ScoredCursor<Cursor>(std::move(cursor), std::move(term_scorer), weight),
          m_max_score(max_score) {
        static_assert((
            concepts::MaxScorePostingCursor<MaxScoredCursor>
            && concepts::SortedPostingCursor<MaxScoredCursor>
        ));
    }
    MaxScoredCursor(MaxScoredCursor const&) = delete;
    MaxScoredCursor(MaxScoredCursor&&) = default;
    MaxScoredCursor& operator=(MaxScoredCursor const&) = delete;
    MaxScoredCursor& operator=(MaxScoredCursor&&) = default;
    ~MaxScoredCursor() = default;

    [[nodiscard]] PISA_ALWAYSINLINE auto max_score() const noexcept -> float {
        return this->weight() * m_max_score;
    }

  private:
    float m_max_score;
};

template <typename Index, typename WandType, typename Scorer>
[[nodiscard]] auto make_max_scored_cursors(
    Index const& index, WandType const& wdata, Scorer const& scorer, Query const& query, bool weighted = false
) {
    std::vector<MaxScoredCursor<typename Index::document_enumerator>> cursors;
    cursors.reserve(query.terms().size());
    std::transform(
        query.terms().begin(),
        query.terms().end(),
        std::back_inserter(cursors),
        [&](WeightedTerm const& term) {
            return MaxScoredCursor<typename Index::document_enumerator>(
                index[term.id],
                scorer.term_scorer(term.id),
                weighted ? term.weight : 1.0F,
                wdata.max_term_weight(term.id)
            );
        }
    );
    return cursors;
}

}  // namespace pisa
