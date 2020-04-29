#pragma once

#include <vector>

#include "query/queries.hpp"
#include "scored_cursor.hpp"
#include "scorer/index_scorer.hpp"
#include "wand_data.hpp"

namespace pisa {

template <typename Cursor, typename Wand>
class BlockMaxScoredCursor: public ScoredCursor<Cursor> {
  public:
    using base_cursor_type = Cursor;

    BlockMaxScoredCursor(
        Cursor cursor,
        TermScorer term_scorer,
        float query_weight,
        float max_score,
        typename Wand::wand_data_enumerator wdata)
        : ScoredCursor<Cursor>(std::move(cursor), std::move(term_scorer), query_weight, max_score),
          m_wdata(std::move(wdata))
    {}
    BlockMaxScoredCursor(BlockMaxScoredCursor const&) = delete;
    BlockMaxScoredCursor(BlockMaxScoredCursor&&) = default;
    BlockMaxScoredCursor& operator=(BlockMaxScoredCursor const&) = delete;
    BlockMaxScoredCursor& operator=(BlockMaxScoredCursor&&) = default;
    ~BlockMaxScoredCursor() = default;

    [[nodiscard]] auto block_max_score() -> float { return m_wdata.score(); }
    [[nodiscard]] auto block_max_docid() -> std::uint32_t { return m_wdata.docid(); }
    void block_max_move(std::uint32_t docid) { m_wdata.next_geq(docid); }

  private:
    typename Wand::wand_data_enumerator m_wdata;
};

template <typename Index, typename WandType, typename Scorer>
[[nodiscard]] auto make_block_max_scored_cursors(
    Index const& index, WandType const& wdata, Scorer const& scorer, Query query)
{
    auto terms = query.terms;
    auto query_term_freqs = query_freqs(terms);

    std::vector<BlockMaxScoredCursor<typename Index::document_enumerator, WandType>> cursors;
    cursors.reserve(query_term_freqs.size());
    std::transform(
        query_term_freqs.begin(), query_term_freqs.end(), std::back_inserter(cursors), [&](auto&& term) {
            float query_weight = term.second;
            auto max_weight = query_weight * wdata.max_term_weight(term.first);
            return BlockMaxScoredCursor<typename Index::document_enumerator, WandType>(
                std::move(index[term.first]),
                scorer.term_scorer(term.first),
                query_weight,
                max_weight,
                wdata.getenum(term.first));
        });
    return cursors;
}

}  // namespace pisa
