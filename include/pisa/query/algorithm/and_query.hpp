#pragma once

#include <algorithm>
#include <cstdint>
#include <vector>

#include "query/queries.hpp"
#include "util/do_not_optimize_away.hpp"

namespace pisa {

struct and_query {
    template <typename CursorRange>
    auto operator()(CursorRange&& cursors, uint32_t max_docid) const
    {
        using Cursor = typename std::decay_t<CursorRange>::value_type;

        using Result_t = uint32_t;

        std::vector<Result_t> results;
        if (cursors.empty()) {
            return results;
        }

        std::vector<Cursor*> ordered_cursors;
        ordered_cursors.reserve(cursors.size());
        for (auto& en: cursors) {
            ordered_cursors.push_back(&en);
        }

        // sort by increasing frequency
        std::sort(ordered_cursors.begin(), ordered_cursors.end(), [](Cursor* lhs, Cursor* rhs) {
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

struct scored_and_query {
    template <typename CursorRange>
    auto operator()(CursorRange&& cursors, uint32_t max_docid) const
    {
        using Cursor = typename std::decay_t<CursorRange>::value_type;

        using Document = uint32_t;
        using Score = float;
        using Result = std::pair<Document, Score>;

        std::vector<Result> results;
        if (cursors.empty()) {
            return results;
        }

        std::vector<Cursor*> ordered_cursors;
        ordered_cursors.reserve(cursors.size());
        for (auto& en: cursors) {
            ordered_cursors.push_back(&en);
        }

        // sort by increasing frequency
        std::sort(ordered_cursors.begin(), ordered_cursors.end(), [](Cursor* lhs, Cursor* rhs) {
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
                auto score = 0.0F;
                for (i = 0; i < ordered_cursors.size(); ++i) {
                    score += ordered_cursors[i]->score();
                }
                results.emplace_back(candidate, score);

                ordered_cursors[0]->next();
                candidate = ordered_cursors[0]->docid();
                i = 1;
            }
        }
        return results;
    }
};

}  // namespace pisa
