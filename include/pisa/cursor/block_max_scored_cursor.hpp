#pragma once

#include <vector>

#include "cursor/max_scored_cursor.hpp"
#include "query.hpp"
#include "scorer/index_scorer.hpp"
#include "util/compiler_attribute.hpp"

namespace pisa {

template <typename Cursor, typename Wand>
    requires(concepts::FrequencyPostingCursor<Cursor> && concepts::SortedPostingCursor<Cursor>)
class BlockMaxScoredCursor: public MaxScoredCursor<Cursor> {
  public:
    using base_cursor_type = Cursor;

    BlockMaxScoredCursor(
        Cursor cursor,
        TermScorer term_scorer,
        float weight,
        float max_score,
        typename Wand::wand_data_enumerator wdata
    )
        : MaxScoredCursor<Cursor>(std::move(cursor), std::move(term_scorer), weight, max_score),
          m_wdata(std::move(wdata)) {
        static_assert(concepts::BlockMaxPostingCursor<BlockMaxScoredCursor>);
    }
    BlockMaxScoredCursor(BlockMaxScoredCursor const&) = delete;
    BlockMaxScoredCursor(BlockMaxScoredCursor&&) = default;
    BlockMaxScoredCursor& operator=(BlockMaxScoredCursor const&) = delete;
    BlockMaxScoredCursor& operator=(BlockMaxScoredCursor&&) = default;
    ~BlockMaxScoredCursor() = default;

    [[nodiscard]] PISA_ALWAYSINLINE auto block_max_score() -> float {
        return m_wdata.score() * this->weight();
    }

    [[nodiscard]] PISA_ALWAYSINLINE auto block_max_docid() -> std::uint32_t {
        return m_wdata.docid();
    }

    PISA_ALWAYSINLINE void block_max_next_geq(std::uint32_t docid) { m_wdata.next_geq(docid); }

  private:
    typename Wand::wand_data_enumerator m_wdata;
};

template <typename Index, typename WandType, typename Scorer>
[[nodiscard]] auto make_block_max_scored_cursors(
    Index const& index, WandType const& wdata, Scorer const& scorer, Query const& query, bool weighted = false
) {
    std::vector<BlockMaxScoredCursor<typename Index::document_enumerator, WandType>> cursors;
    cursors.reserve(query.terms().size());
    std::transform(
        query.terms().begin(),
        query.terms().end(),
        std::back_inserter(cursors),
        [&](WeightedTerm const& term) {
            return BlockMaxScoredCursor<typename Index::document_enumerator, WandType>(
                index[term.id],
                scorer.term_scorer(term.id),
                weighted ? term.weight : 1.0F,
                wdata.max_term_weight(term.id),
                wdata.getenum(term.id)
            );
        }
    );

    return cursors;
}

}  // namespace pisa
