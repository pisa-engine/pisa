#pragma once

#include <vector>

#include "query/queries.hpp"
#include "scorer/bm25.hpp"
#include "scorer/index_scorer.hpp"
#include "wand_data.hpp"

namespace pisa {

template <typename Index, typename WandType>
struct block_max_scored_cursor {
    using enum_type = typename Index::document_enumerator;
    using wdata_enum = typename WandType::wand_data_enumerator;

    enum_type docs_enum;
    wdata_enum w;
    float q_weight;
    term_scorer_t scorer;
    float max_weight;
};

template <typename Index, typename WandType, typename Scorer>
[[nodiscard]] auto make_block_max_scored_cursors(Index const& index,
                                                 WandType const& wdata,
                                                 Scorer const& scorer,
                                                 Query query)
{
    auto terms = query.terms;
    auto query_term_freqs = query_freqs(terms);

    std::vector<block_max_scored_cursor<Index, WandType>> cursors;
    cursors.reserve(query_term_freqs.size());
    std::transform(
        query_term_freqs.begin(),
        query_term_freqs.end(),
        std::back_inserter(cursors),
        [&](auto&& term) {
            auto list = index[term.first];
            auto w_enum = wdata.getenum(term.first);
            float q_weight = term.second;
            auto max_weight = q_weight * wdata.max_term_weight(term.first);
            return block_max_scored_cursor<Index, WandType>{
                std::move(list), w_enum, q_weight, scorer.term_scorer(term.first), max_weight};
        });
    return cursors;
}

} // namespace pisa
