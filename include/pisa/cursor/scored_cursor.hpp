#pragma once

#include <vector>
#include "query/queries.hpp"
#include "scorer/bm25.hpp"
#include "scorer/score_function.hpp"

namespace pisa {

template <typename Index, typename Scorer>
struct scored_cursor {
    using enum_type = typename Index::document_enumerator;
    enum_type docs_enum;
    float     q_weight;
    Scorer    scorer;
};

template <typename Index, typename WandType>
[[nodiscard]] auto make_scored_cursors(Index const &index, WandType const &wdata,
                                       Query query) {
    auto terms = query.terms;
    auto query_term_freqs = query_freqs(terms);
    using scorer_type     = bm25;
    using Scorer          = Score_Function<scorer_type, WandType>;

    std::vector<scored_cursor<Index, Scorer>> cursors;
    cursors.reserve(query_term_freqs.size());
    std::transform(
        query_term_freqs.begin(), query_term_freqs.end(), std::back_inserter(cursors),
        [&](auto &&term) {
            auto list = index[term.first];
            auto q_weight =
                scorer_type::query_term_weight(term.second, wdata.term_len(term.first), index.num_docs());
            return scored_cursor<Index, Scorer>{std::move(list), q_weight, {q_weight, wdata}};
        });
    return cursors;
}

}  // namespace pisa
