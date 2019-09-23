#pragma once

#include <vector>

#include <boost/preprocessor/cat.hpp>
#include <boost/preprocessor/seq/for_each.hpp>

#include "codec/list.hpp"
#include "query/queries.hpp"

namespace pisa {

template <typename Index>
[[nodiscard]] auto make_cursors(Index const &index, Query query) {
    auto terms = query.terms;
    remove_duplicate_terms(terms);
    using cursor          = typename Index::document_enumerator;

    std::vector<cursor> cursors;
    cursors.reserve(terms.size());
    std::transform(terms.begin(), terms.end(), std::back_inserter(cursors),
                   [&](auto &&term) { return index[term]; });

    return cursors;
}

#define LOOP_BODY(R, DATA, INDEX)                                   \
    extern template auto make_cursors<BOOST_PP_CAT(INDEX, _index)>( \
        BOOST_PP_CAT(INDEX, _index) const &, Query);
/**/
BOOST_PP_SEQ_FOR_EACH(LOOP_BODY, _, PISA_INDEX_TYPES);
#undef LOOP_BODY

}  // namespace pisa
