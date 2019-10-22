#pragma once

#include <vector>
#include "query/queries.hpp"
#include "scorer/bm25.hpp"

namespace pisa {

template <typename Index, typename WandType, typename Scorer>
struct block_max_scored_cursor {
    using enum_type  = typename Index::document_enumerator;
    using wdata_enum = typename WandType::wand_data_enumerator;

    enum_type  docs_enum;
    wdata_enum w;
    float      q_weight;
    Scorer     scorer;
    float      max_weight;
};

template <typename Index, typename WandType>
[[nodiscard]] auto make_block_max_scored_cursors(Index const &index, WandType const &wdata,
                                                 Query query) {
    auto terms = query.terms;
    auto query_term_freqs = query_freqs(terms);
    using scorer_type     = bm25;
    using Scorer          = Score_Function<scorer_type, WandType>;

    std::vector<block_max_scored_cursor<Index, WandType, Scorer>> cursors;
    cursors.reserve(query_term_freqs.size());
    std::transform(query_term_freqs.begin(), query_term_freqs.end(), std::back_inserter(cursors),
                   [&](auto &&term) {
                       auto list       = index[term.first];
                       auto w_enum     = wdata.getenum(term.first);
                       auto q_weight   = scorer_type::query_term_weight(term.second, wdata.term_len(term.first),
                                                                      index.num_docs());
                       auto max_weight = q_weight * wdata.max_term_weight(term.first);
                       return block_max_scored_cursor<Index, WandType, Scorer>{
                           std::move(list), w_enum, q_weight, {q_weight, wdata}, max_weight};
                   });
    return cursors;
}

}  // namespace pisa