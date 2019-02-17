#pragma once

#include <vector>

namespace pisa {

template <typename Index>
[[nodiscard]] auto make_cursors(Index const &index, term_id_vec terms) {
    auto query_term_freqs = query_freqs(terms);
    typedef typename Index::document_enumerator cursor;

    std::vector<cursor> cursors;
    cursors.reserve(query_term_freqs.size());

    for (auto term : query_term_freqs) {
        cursors.push_back(index[term.first]);
    }
    return cursors;
}

}  // namespace pisa
