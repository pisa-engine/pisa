#pragma once

#include <vector>

#include "cursor/max_scored_cursor.hpp"
#include "query.hpp"
#include "scorer/index_scorer.hpp"
#include "wand_data.hpp"

namespace pisa {

template <typename Cursor, typename Wand>
class BlockMaxScoredCursor: public MaxScoredCursor<Cursor> {
  public:
    using base_cursor_type = Cursor;

    BlockMaxScoredCursor(
        Cursor cursor,
        TermScorer term_scorer,
        float weight,
        float max_score,
        typename Wand::wand_data_enumerator wdata)
        : MaxScoredCursor<Cursor>(std::move(cursor), std::move(term_scorer), weight, max_score),
          m_wdata(std::move(wdata))
    {}
    BlockMaxScoredCursor(BlockMaxScoredCursor const&) = delete;
    BlockMaxScoredCursor(BlockMaxScoredCursor&&) = default;
    BlockMaxScoredCursor& operator=(BlockMaxScoredCursor const&) = delete;
    BlockMaxScoredCursor& operator=(BlockMaxScoredCursor&&) = default;
    ~BlockMaxScoredCursor() = default;

    [[nodiscard]] PISA_ALWAYSINLINE auto block_max_score() -> float { return m_wdata.score(); }
    [[nodiscard]] PISA_ALWAYSINLINE auto block_max_docid() -> std::uint32_t
    {
        return m_wdata.docid();
    }
    PISA_ALWAYSINLINE void block_max_next_geq(std::uint32_t docid) { m_wdata.next_geq(docid); }

  private:
    typename Wand::wand_data_enumerator m_wdata;
};

template <typename Index, typename WandType, typename Scorer>
[[nodiscard]] auto make_block_max_scored_cursors(
    Index const& index, WandType const& wdata, Scorer const& scorer, QueryRequest query)
{
    using cursor_type = BlockMaxScoredCursor<typename Index::document_enumerator, WandType>;
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
            return cursor_type(
                index[term_id], scorer.term_scorer(term_id), weight, max_weight, wdata.getenum(term_id));
        });
    return cursors;
}

}  // namespace pisa
