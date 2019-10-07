#pragma once

#include <vector>

#include <gsl/span>

#include "cursor/cursor.hpp"
#include "macro.hpp"
#include "query/queries.hpp"
#include "util/do_not_optimize_away.hpp"

namespace pisa {

template <bool with_freqs = false>
struct and_query {
    using Result = std::conditional<with_freqs, std::pair<float, std::uint32_t>, std::uint32_t>;
    std::vector<Result> results;

    template <typename Cursor>
    auto operator()(gsl::span<Cursor> cursors, uint64_t max_docid) const -> uint64_t
    {
        if (cursors.empty()) {
            return 0;
        }
        results.clear();

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

        if constexpr (with_freqs) {
            while (candidate < max_docid) {
                float score(0);
                for (; i < ordered_cursors.size(); ++i) {
                    ordered_cursors[i]->next_geq(candidate);
                    if (ordered_cursors[i]->docid() != candidate) {
                        candidate = ordered_cursors[i]->docid();
                        i = 0;
                        break;
                    }
                    score += ordered_cursors[i]->freq();
                }
                if (i == ordered_cursors.size()) {
                    results.push_back({score, candidate});
                    ordered_cursors[0]->next();
                    candidate = ordered_cursors[0]->docid();
                    i = 1;
                }
            }
        } else {
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
        }
        return results.size();
    }
};

} // namespace pisa
