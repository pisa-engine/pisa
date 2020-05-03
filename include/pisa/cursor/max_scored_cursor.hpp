#pragma once

#include <vector>

#include <range/v3/view/zip.hpp>

#include "query.hpp"
#include "query/queries.hpp"
#include "scorer/index_scorer.hpp"
#include "wand_data.hpp"

namespace pisa {

template <typename Index>
struct max_scored_cursor {
    using enum_type = typename Index::document_enumerator;
    max_scored_cursor() = delete;
    enum_type docs_enum;
    float q_weight;
    term_scorer_t scorer;
    float max_weight;
};

template <typename Index, typename WandType, typename Scorer>
[[nodiscard]] auto make_max_scored_cursors(
    Index const& index, WandType const& wdata, Scorer const& scorer, QueryRequest query)
{
    auto term_ids = query.term_ids();
    auto term_weights = query.term_ids();

    std::vector<max_scored_cursor<Index>> cursors;
    cursors.reserve(term_ids.size());

    for (auto [term_id, term_weight]: ranges::views::zip(term_ids, term_weights)) {
        cursors.push_back(
            max_scored_cursor<Index>{index[term_id], term_weight * wdata.max_term_weight(term_id)});
    }
    return cursors;
}

}  // namespace pisa
