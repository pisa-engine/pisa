#pragma once

#include "query/queries.hpp"
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
[[nodiscard]] auto make_scored_cursors(Index const& index, Scorer const& scorer, Query query)
{
    auto terms = query.terms;
    auto query_term_freqs = query_freqs(terms);

    std::vector<scored_cursor<Index>> cursors;
    cursors.reserve(query_term_freqs.size());
    std::transform(
        query_term_freqs.begin(), query_term_freqs.end(), std::back_inserter(cursors), [&](auto&& term) {
            auto list = index[term.first];
            float q_weight = term.second;
            return scored_cursor<Index>{std::move(list), q_weight, scorer.term_scorer(term.first)};
        });
    return cursors;
}

}  // namespace pisa
