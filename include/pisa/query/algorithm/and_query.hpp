#pragma once

#include <vector>
#include "query/queries.hpp"
#include "util/do_not_optimize_away.hpp"

namespace pisa {

template <bool with_freqs>
struct and_query {

    template<typename CursorRange>
    uint64_t operator()(CursorRange &&cursors) const {
        using Cursor = typename CursorRange::value_type;
        if (cursors.empty())
            return 0;

        std::vector<Cursor *> ordered_cursors;
        ordered_cursors.reserve(cursors.size());
        for (auto &en : cursors) {
            ordered_cursors.push_back(&en);
        }


        // sort by increasing frequency
        std::sort(ordered_cursors.begin(), ordered_cursors.end(), [](Cursor *lhs, Cursor *rhs) {
            return lhs->size() < rhs->size();
        });

        uint64_t results   = 0;
        uint64_t candidate = ordered_cursors[0]->docid();
        size_t   i         = 1;
        while (candidate < m_max_docid) {
            for (; i < ordered_cursors.size(); ++i) {
                ordered_cursors[i]->next_geq(candidate);
                if (ordered_cursors[i]->docid() != candidate) {
                    candidate = ordered_cursors[i]->docid();
                    i         = 0;
                    break;
                }
            }

            if (i == ordered_cursors.size()) {
                results += 1;

                if constexpr (with_freqs) {
                    for (i = 0; i < ordered_cursors.size(); ++i) {
                        do_not_optimize_away(ordered_cursors[i]->freq());
                    }
                }
                ordered_cursors[0]->next();
                candidate = ordered_cursors[0]->docid();
                i         = 1;
            }
        }
        return results;
    }

};

} // namespace pisa