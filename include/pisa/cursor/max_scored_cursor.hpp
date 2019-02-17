#pragma once

#include <vector>

namespace pisa {

template <typename Index>
struct max_scored_cursor {
    typedef typename Index::document_enumerator enum_type;
    enum_type                                   docs_enum;
    float                                       q_weight;
    float                                       max_weight;
};

template <typename Index, typename WandType, typename scorer_type = bm25>
[[nodiscard]] auto make_max_scored_cursors(Index const &index, WandType const &wdata,
                                       term_id_vec terms) {
    auto query_term_freqs = query_freqs(terms);

    std::vector<max_scored_cursor<Index>> cursors;
    cursors.reserve(query_term_freqs.size());

    for (auto term : query_term_freqs) {
        auto list     = index[term.first];
        auto q_weight = scorer_type::query_term_weight(term.second, list.size(), index.num_docs());
        auto max_weight = q_weight * wdata.max_term_weight(term.first);
        cursors.push_back(max_scored_cursor<Index>{std::move(list), q_weight, max_weight});
    }
    return cursors;
}

}  // namespace pisa
