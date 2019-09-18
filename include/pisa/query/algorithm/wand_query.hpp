#pragma once

#include <vector>

#include <gsl/span>

#include "cursor/max_scored_cursor.hpp"
#include "macro.hpp"
#include "util/do_not_optimize_away.hpp"
#include "query/queries.hpp"
#include "topk_queue.hpp"

namespace pisa {

struct wand_query {

    wand_query(uint64_t k) : m_topk(k) {}

    template <typename Cursor>
    uint64_t operator()(gsl::span<Cursor> cursors, uint64_t max_docid)
    {
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
            std::sort(ordered_cursors.begin(), ordered_cursors.end(), [](Cursor *lhs, Cursor *rhs) {
                return lhs->docs_enum.docid() < rhs->docs_enum.docid();
            });
        };

        sort_enums();
        while (true) {
            // find pivot
            float upper_bound = 0;
            size_t pivot;
            bool found_pivot = false;
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
                float score = 0;
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
                for (; ordered_cursors[next_list]->docs_enum.docid() == pivot_id; --next_list) {
                }
                ordered_cursors[next_list]->docs_enum.next_geq(pivot_id);
                // bubble down the advanced list
                for (size_t i = next_list + 1; i < ordered_cursors.size(); ++i) {
                    if (ordered_cursors[i]->docs_enum.docid()
                        < ordered_cursors[i - 1]->docs_enum.docid()) {
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
    TopKQueue m_topk;
};

template <typename Index, typename TermScorer>
struct scored_cursor;

#define LOOP_BODY(R, DATA, T)                                                 \
    PISA_DAAT_MAX_ALGORITHM_EXTERN(wand_query, bm25, T, wand_data_raw)        \
    PISA_DAAT_MAX_ALGORITHM_EXTERN(wand_query, dph, T, wand_data_raw)         \
    PISA_DAAT_MAX_ALGORITHM_EXTERN(wand_query, pl2, T, wand_data_raw)         \
    PISA_DAAT_MAX_ALGORITHM_EXTERN(wand_query, qld, T, wand_data_raw)         \
    PISA_DAAT_MAX_ALGORITHM_EXTERN(wand_query, bm25, T, wand_data_compressed) \
    PISA_DAAT_MAX_ALGORITHM_EXTERN(wand_query, dph, T, wand_data_compressed)  \
    PISA_DAAT_MAX_ALGORITHM_EXTERN(wand_query, pl2, T, wand_data_compressed)  \
    PISA_DAAT_MAX_ALGORITHM_EXTERN(wand_query, qld, T, wand_data_compressed)  \
/**/
BOOST_PP_SEQ_FOR_EACH(LOOP_BODY, _, PISA_INDEX_TYPES);
#undef LOOP_BODY

} // namespace pisa
