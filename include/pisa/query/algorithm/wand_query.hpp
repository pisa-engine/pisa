#pragma once

#include <vector>

#include "scorer/bm25.hpp"
#include "topk_queue.hpp"
#include "query/queries.hpp"

namespace pisa {

template <typename Index, typename WandType>
struct wand_query {

    typedef bm25 scorer_type;

    wand_query(Index const &index, WandType const &wdata, uint64_t k, uint64_t max_docid)
        : m_index(index), m_wdata(&wdata), m_topk(k), m_max_docid(max_docid) {}

    template<typename Cursor>
    uint64_t operator()(std::vector<Cursor> &&cursors) {
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
                if (ordered_cursors[pivot]->docs_enum.docid() == m_max_docid) {
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
                float norm_len = m_wdata->norm_len(pivot_id);
                for (Cursor *en : ordered_cursors) {
                    if (en->docs_enum.docid() != pivot_id) {
                        break;
                    }
                    score +=
                        en->q_weight * scorer_type::doc_term_weight(en->docs_enum.freq(), norm_len);
                    en->docs_enum.next();
                }

                m_topk.insert(score, pivot_id);
                // resort by docid
                sort_enums();
            } else {
                // no match, move farthest list up to the pivot
                uint64_t next_list = pivot;
                for (; ordered_cursors[next_list]->docs_enum.docid() == pivot_id; --next_list)
                    ;
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
    Index const &   m_index;
    WandType const *m_wdata;
    topk_queue      m_topk;
    uint64_t        m_max_docid;
};

} // namespace pisa