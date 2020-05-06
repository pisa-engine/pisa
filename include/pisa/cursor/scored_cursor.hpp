#pragma once

#include "query.hpp"
#include "scorer/index_scorer.hpp"
#include "wand_data.hpp"
#include <vector>

namespace pisa {

template <typename Index>
struct scored_cursor {
    using enum_type = typename Index::document_enumerator;
    scored_cursor() = delete;
    enum_type docs_enum;
    float q_weight;
    term_scorer_t scorer;
};

template <typename Index, typename Scorer>
[[nodiscard]] auto make_scored_cursors(Index const& index, Scorer const& scorer, QueryRequest query)
{
    auto term_ids = query.term_ids();
    auto term_weights = query.term_weights();
    std::vector<scored_cursor<Index>> cursors;
    cursors.reserve(term_ids.size());
    std::transform(
        term_ids.begin(),
        term_ids.end(),
        term_weights.begin(),
        std::back_inserter(cursors),
        [&](auto term_id, auto weight) {
            return scored_cursor<Index>{index[term_id], weight, scorer.term_scorer(term_id)};
        });
    return cursors;
}

}  // namespace pisa
