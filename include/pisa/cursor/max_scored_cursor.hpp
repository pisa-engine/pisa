#pragma once

#include <vector>

#include "cursor/scored_cursor.hpp"
#include "query.hpp"
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
}

}  // namespace pisa
