#pragma once

#include <vector>

#include "macro.hpp"
#include "query/queries.hpp"
#include "util/do_not_optimize_away.hpp"

namespace pisa {

struct and_query {

    template <typename Cursor>
    auto operator()(std::vector<Cursor> &&cursors, uint64_t max_docid) const
        -> std::vector<uint64_t>
    {
        using Result_t = uint64_t;

        std::vector<Result_t> results;
        if (cursors.empty())
            return results;

        std::vector<Cursor *> ordered_cursors;
        ordered_cursors.reserve(cursors.size());
        for (auto &en : cursors) {
            ordered_cursors.push_back(&en);
        }

        // sort by increasing frequency
        std::sort(ordered_cursors.begin(), ordered_cursors.end(), [](Cursor *lhs, Cursor *rhs) {
            return lhs->size() < rhs->size();
        });

        uint32_t candidate = ordered_cursors[0]->docid();
        size_t i = 1;

        while (candidate < max_docid) {
            for (; i < ordered_cursors.size(); ++i) {
                ordered_cursors[i]->next_geq(candidate);
                if (ordered_cursors[i]->docid() != candidate) {
                    candidate = ordered_cursors[i]->docid();
                    i = 0;
                    break;
                }
            }

            if (i == ordered_cursors.size()) {

                results.push_back(candidate);

                ordered_cursors[0]->next();
                candidate = ordered_cursors[0]->docid();
                i = 1;
            }
        }
        return results;
    }
};

#define LOOP_BODY(R, DATA, T)                                                      \
    extern template std::vector<uint64_t> and_query::                              \
    operator()<typename block_freq_index<T>::document_enumerator>(                 \
        std::vector<typename block_freq_index<T>::document_enumerator> && cursors, \
        uint64_t max_docid) const;
/**/
BOOST_PP_SEQ_FOR_EACH(LOOP_BODY, _, PISA_BLOCK_CODEC_TYPES);
#undef LOOP_BODY

} // namespace pisa
