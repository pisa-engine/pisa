#pragma once

#include <vector>
#include "query/queries.hpp"
#include "util/do_not_optimize_away.hpp"

namespace pisa {

template <bool with_freqs>
struct and_query {

    template<typename CursorRange>
    auto operator()(CursorRange &&cursors, uint32_t max_docid) const {
        using Cursor = typename CursorRange::value_type;

        using Doc_t = uint32_t;
        using Score_t = float;
        using DocScore_t = std::pair<Doc_t, Score_t>;
        using Result_t = typename std::conditional<with_freqs, DocScore_t, Doc_t>::type;

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
            return lhs->docs_enum.size() < rhs->docs_enum.size();
        });

        uint32_t candidate = ordered_cursors[0]->docs_enum.docid();
        size_t   i         = 1;



        while (candidate < max_docid) {
            for (; i < ordered_cursors.size(); ++i) {
                ordered_cursors[i]->docs_enum.next_geq(candidate);
                if (ordered_cursors[i]->docs_enum.docid() != candidate) {
                    candidate = ordered_cursors[i]->docs_enum.docid();
                    i         = 0;
                    break;
                }
            }

            if (i == ordered_cursors.size()) {

                if constexpr (with_freqs) {
                    auto score = 0.0f;
                    for (i = 0; i < ordered_cursors.size(); ++i) {
                        score += ordered_cursors[i]->scorer(ordered_cursors[i]->docs_enum.docid(), ordered_cursors[i]->docs_enum.freq());
                    }
                    results.emplace_back(candidate, score);
                } else {
                    results.push_back(candidate);
                }

                ordered_cursors[0]->docs_enum.next();
                candidate = ordered_cursors[0]->docs_enum.docid();
                i         = 1;
            }
        }
        return results;
    }

};

} // namespace pisa