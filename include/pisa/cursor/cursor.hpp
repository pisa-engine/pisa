#pragma once

#include <vector>

#include "query.hpp"

namespace pisa {

/** Creates cursors for query terms.
 *
 * These are frequency-only cursors. If you want to calculate scores, use
 * `make_scored_cursors`, `make_max_scored_cursors`, or `make_block_max_scored_cursors`.
 */
template <typename Index>
[[nodiscard]] auto make_cursors(Index const& index, Query query) {
    using cursor = typename Index::document_enumerator;

    std::vector<cursor> cursors;
    cursors.reserve(query.terms().size());
    std::transform(
        query.terms().begin(),
        query.terms().end(),
        std::back_inserter(cursors),
        [&](auto&& term) { return index[term.id]; }
    );

    return cursors;
}

}  // namespace pisa
