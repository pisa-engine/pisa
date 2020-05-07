#pragma once

#include <vector>

#include "query.hpp"

namespace pisa {

template <typename Index>
[[nodiscard]] auto make_cursors(Index const& index, QueryRequest query)
{
    auto term_ids = query.term_ids();
    std::vector<typename Index::document_enumerator> cursors;
    cursors.reserve(term_ids.size());
    std::transform(term_ids.begin(), term_ids.end(), std::back_inserter(cursors), [&](auto&& term_id) {
        return index[term_id];
    });

    return cursors;
}

}  // namespace pisa
