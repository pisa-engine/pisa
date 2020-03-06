#pragma once

#include "query/queries.hpp"
#include <vector>

namespace pisa {

template <typename Index>
[[nodiscard]] auto make_cursors(Index const& index, Query query)
{
    auto terms = query.terms;
    remove_duplicate_terms(terms);
    using cursor = typename Index::document_enumerator;

    std::vector<cursor> cursors;
    cursors.reserve(terms.size());
    std::transform(terms.begin(), terms.end(), std::back_inserter(cursors), [&](auto&& term) {
        return index[term];
    });

    return cursors;
}

}  // namespace pisa
