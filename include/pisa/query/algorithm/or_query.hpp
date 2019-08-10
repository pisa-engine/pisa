#pragma once

#include <vector>

#include <gsl/span>

#include "macro.hpp"
#include "query/queries.hpp"
#include "util/do_not_optimize_away.hpp"

namespace pisa {

template <bool with_freqs>
struct or_query {

    template <typename Cursor>
    uint64_t operator()(gsl::span<Cursor> cursors, uint64_t max_docid) const
    {
        if (cursors.empty()) {
            return 0;
        }

        uint64_t results = 0;
        uint64_t cur_doc = std::min_element(cursors.begin(),
                                            cursors.end(),
                                            [](Cursor const &lhs, Cursor const &rhs) {
                                                return lhs.docid() < rhs.docid();
                                            })
                               ->docid();

        while (cur_doc < max_docid) {
            results += 1;
            uint64_t next_doc = max_docid;
            for (size_t i = 0; i < cursors.size(); ++i) {
                if (cursors[i].docid() == cur_doc) {
                    if constexpr (with_freqs) {
                        do_not_optimize_away(cursors[i].freq());
                    }
                    cursors[i].next();
                }
                if (cursors[i].docid() < next_doc) {
                    next_doc = cursors[i].docid();
                }
            }

            cur_doc = next_doc;
        }

        return results;
    }
};

#define LOOP_BODY(R, DATA, INDEX)                                                     \
    extern template uint64_t or_query<false>::                                        \
    operator()<typename BOOST_PP_CAT(INDEX, _index)::document_enumerator>(            \
        gsl::span<typename BOOST_PP_CAT(INDEX, _index)::document_enumerator> cursors, \
        uint64_t max_docid) const;                                                    \
    extern template uint64_t or_query<true>::                                         \
    operator()<typename BOOST_PP_CAT(INDEX, _index)::document_enumerator>(            \
        gsl::span<typename BOOST_PP_CAT(INDEX, _index)::document_enumerator> cursors, \
        uint64_t max_docid) const;
/**/
BOOST_PP_SEQ_FOR_EACH(LOOP_BODY, _, PISA_INDEX_TYPES);
#undef LOOP_BODY

} // namespace pisa
