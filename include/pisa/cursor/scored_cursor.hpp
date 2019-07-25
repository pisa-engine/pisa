#pragma once

#include "query/queries.hpp"
#include "scorer/index_scorer.hpp"
#include <vector>

namespace pisa {

template <typename Index, typename TermScorer>
struct scored_cursor {
    using enum_type = typename Index::document_enumerator;
    enum_type docs_enum;
    float q_weight;
    TermScorer scorer;
};

template <typename Index, typename Scorer>
[[nodiscard]] auto make_scored_cursors(Index const &index, Scorer const &scorer, Query query)
{
    auto terms = query.terms;
    auto query_term_freqs = query_freqs(terms);
    using term_scorer_type = typename scorer_traits<Scorer>::term_scorer;

    std::vector<scored_cursor<Index, term_scorer_type>> cursors;
    cursors.reserve(query_term_freqs.size());
    std::transform(
        query_term_freqs.begin(),
        query_term_freqs.end(),
        std::back_inserter(cursors),
        [&](auto &&term) {
            auto list = index[term.first];
            float q_weight = term.second;
            return scored_cursor<Index, term_scorer_type>{
                std::move(list), q_weight, scorer.term_scorer(term.first)};
        });
    return cursors;
}

} // namespace pisa
