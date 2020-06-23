#pragma once

#include <vector>

#include "cursor/max_scored_cursor.hpp"
#include "query/queries.hpp"
#include "scorer/index_scorer.hpp"
#include "wand_data.hpp"

namespace pisa {

template <typename Cursor, typename Wand>
class RangeBlockMaxScoredCursor: public MaxScoredCursor<Cursor> {
  public:
    using base_cursor_type = Cursor;

    RangeBlockMaxScoredCursor(
        Cursor cursor,
        TermScorer term_scorer,
        float weight,
        float max_score,
        std::vector<uint16_t> &scores)
        : MaxScoredCursor<Cursor>(std::move(cursor), std::move(term_scorer), weight, max_score),
         m_scores(scores)
    {}
    RangeBlockMaxScoredCursor(RangeBlockMaxScoredCursor const&) = delete;
    RangeBlockMaxScoredCursor(RangeBlockMaxScoredCursor&&) = default;
    RangeBlockMaxScoredCursor& operator=(RangeBlockMaxScoredCursor const&) = delete;
    RangeBlockMaxScoredCursor& operator=(RangeBlockMaxScoredCursor&&) = default;
    ~RangeBlockMaxScoredCursor() = default;

    [[nodiscard]] PISA_ALWAYSINLINE auto scores(size_t id) -> float { return m_scores[id]; }

  private:
    std::vector<uint16_t> &m_scores;
};

template <typename Index, typename WandType, typename Scorer>
[[nodiscard]] auto make_range_block_max_scored_cursors(
    Index const& index, WandType const& wdata, Scorer const& scorer, Query query, std::map<uint32_t, std::vector<uint16_t>> &term_enum)
{
    auto terms = query.terms;
    auto query_term_freqs = query_freqs(terms);

    std::vector<RangeBlockMaxScoredCursor<typename Index::document_enumerator, WandType>> cursors;
    cursors.reserve(query_term_freqs.size());
    std::transform(
        query_term_freqs.begin(), query_term_freqs.end(), std::back_inserter(cursors), [&](auto&& term) {
            float weight = term.second;
            auto max_weight = weight * wdata.max_term_weight(term.first);
            return RangeBlockMaxScoredCursor<typename Index::document_enumerator, WandType>(
                std::move(index[term.first]),
                scorer.term_scorer(term.first),
                weight,
                max_weight,
                term_enum[term.first]);
        });
    return cursors;
}

}  // namespace pisa
