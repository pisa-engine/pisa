#pragma once

#include <vector>

#include "query.hpp"
#include "scorer/index_scorer.hpp"
#include "wand_data.hpp"

namespace pisa {

template <typename Index, typename WandType>
struct block_max_scored_cursor {
    using enum_type = typename Index::document_enumerator;
    using wdata_enum = typename WandType::wand_data_enumerator;

    block_max_scored_cursor() = delete;

    enum_type docs_enum;
    wdata_enum w;
    float q_weight;
    term_scorer_t scorer;
    float max_weight;
};

template <typename Index, typename WandType, typename Scorer>
[[nodiscard]] auto make_block_max_scored_cursors(
    Index const& index, WandType const& wdata, Scorer const& scorer, QueryRequest query)
{
    using cursor_type = block_max_scored_cursor<Index, WandType>;
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
            return cursor_type{
                index[term_id], wdata.getenum(term_id), weight, scorer.term_scorer(term_id), max_weight};
        });
    return cursors;
}

}  // namespace pisa
