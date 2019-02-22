#pragma once

#include <vector>
#include "query/queries.hpp"

namespace pisa {

template <typename Index>
[[nodiscard]] auto make_cursors(Index const &index, term_id_vec terms) {
    remove_duplicate_terms(terms);
    using cursor          = typename Index::document_enumerator;

    std::vector<cursor> cursors;
    cursors.reserve(terms.size());
    std::transform(terms.begin(), terms.end(), std::back_inserter(cursors),
                   [&](auto &&term) { return index[term]; });

    return cursors;
}

}  // namespace pisa
