#pragma once

#include <vector>

#include "scorer/bm25.hpp"
#include "topk_queue.hpp"
#include "query/queries.hpp"

namespace pisa {

struct wand_query {

    typedef bm25 scorer_type;

    wand_query(uint64_t k)
        : m_topk(k) {}

    template<typename CursorRange>
    uint64_t operator()(CursorRange &&cursors, uint64_t max_docid) {
        using Cursor = typename CursorRange::value_type;
        m_topk.clear();
        if (cursors.empty())
            return 0;

        std::vector<Cursor *> ordered_cursors;
        ordered_cursors.reserve(cursors.size());
        for (auto &en : cursors) {
            ordered_cursors.push_back(&en);
        }

        auto sort_enums = [&]() {
            // sort enumerators by increasing docid
            std::sort(
                ordered_cursors.begin(), ordered_cursors.end(), [](Cursor *lhs, Cursor *rhs) {
                    return lhs->docs_enum.docid() < rhs->docs_enum.docid();
                });
        };

        sort_enums();
        while (true) {
            // find pivot
            float  upper_bound = 0;
            size_t pivot;
            bool   found_pivot = false;
            for (pivot = 0; pivot < ordered_cursors.size(); ++pivot) {
                if (ordered_cursors[pivot]->docs_enum.docid() >= max_docid) {
                    break;
                }
                upper_bound += ordered_cursors[pivot]->max_weight;
                if (m_topk.would_enter(upper_bound)) {
                    found_pivot = true;
                    break;
                }
            }

            // no pivot found, we can stop the search
            if (!found_pivot) {
                break;
            }

            // check if pivot is a possible match
            uint64_t pivot_id = ordered_cursors[pivot]->docs_enum.docid();
            if (pivot_id == ordered_cursors[0]->docs_enum.docid()) {
                float score    = 0;
                for (Cursor *en : ordered_cursors) {
                    if (en->docs_enum.docid() != pivot_id) {
                        break;
                    }
                    score += en->scorer(en->docs_enum.docid(), en->docs_enum.freq());
                    en->docs_enum.next();
                }

                m_topk.insert(score, pivot_id);
                // resort by docid
                sort_enums();
            } else {
                // no match, move farthest list up to the pivot
                uint64_t next_list = pivot;
                for (; ordered_cursors[next_list]->docs_enum.docid() == pivot_id; --next_list) {}
                ordered_cursors[next_list]->docs_enum.next_geq(pivot_id);
                // bubble down the advanced list
                for (size_t i = next_list + 1; i < ordered_cursors.size(); ++i) {
                    if (ordered_cursors[i]->docs_enum.docid() <
                        ordered_cursors[i - 1]->docs_enum.docid()) {
                        std::swap(ordered_cursors[i], ordered_cursors[i - 1]);
                    } else {
                        break;
                    }
                }
            }
        }

        m_topk.finalize();
        return m_topk.topk().size();
    }

    std::vector<std::pair<float, uint64_t>> const &topk() const { return m_topk.topk(); }

   private:
    topk_queue      m_topk;
};

} // namespace pisa