#pragma once

#include <vector>

#include <gsl/span>

#include "cursor/block_max_scored_cursor.hpp"
#include "macro.hpp"
#include "query/queries.hpp"
#include "topk_queue.hpp"

namespace pisa {

struct block_max_wand_query {

    block_max_wand_query(uint64_t k) : m_topk(k) {}

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

        auto sort_cursors = [&]() {
            // sort enumerators by increasing docid
            std::sort(ordered_cursors.begin(), ordered_cursors.end(), [](Cursor *lhs, Cursor *rhs) {
                return lhs->docs_enum.docid() < rhs->docs_enum.docid();
            });
        };

        sort_cursors();

        while (true) {

            // find pivot
            float upper_bound = 0.f;
            size_t pivot;
            bool found_pivot = false;
            uint64_t pivot_id = max_docid;

            for (pivot = 0; pivot < ordered_cursors.size(); ++pivot) {
                if (ordered_cursors[pivot]->docs_enum.docid() >= max_docid) {
                    break;
                }

                upper_bound += ordered_cursors[pivot]->max_weight;
                if (m_topk.would_enter(upper_bound)) {
                    found_pivot = true;
                    pivot_id = ordered_cursors[pivot]->docs_enum.docid();
                    for (; pivot + 1 < ordered_cursors.size()
                           && ordered_cursors[pivot + 1]->docs_enum.docid() == pivot_id;
                         ++pivot)
                        ;
                    break;
                }
            }

            // no pivot found, we can stop the search
            if (!found_pivot) {
                break;
            }

            double block_upper_bound = 0;

            for (size_t i = 0; i < pivot + 1; ++i) {
                if (ordered_cursors[i]->w.docid() < pivot_id) {
                    ordered_cursors[i]->w.next_geq(pivot_id);
                }

                block_upper_bound += ordered_cursors[i]->w.score() * ordered_cursors[i]->q_weight;
            }

            if (m_topk.would_enter(block_upper_bound)) {

                // check if pivot is a possible match
                if (pivot_id == ordered_cursors[0]->docs_enum.docid()) {
                    float score = 0;
                    for (Cursor *en : ordered_cursors) {
                        if (en->docs_enum.docid() != pivot_id) {
                            break;
                        }
                        float part_score = en->scorer(en->docs_enum.docid(), en->docs_enum.freq());
                        score += part_score;
                        block_upper_bound -= en->w.score() * en->q_weight - part_score;
                        if (!m_topk.would_enter(block_upper_bound)) {
                            break;
                        }
                    }
                    for (Cursor *en : ordered_cursors) {
                        if (en->docs_enum.docid() != pivot_id) {
                            break;
                        }
                        en->docs_enum.next();
                    }

                    m_topk.insert(score, pivot_id);
                    // resort by docid
                    sort_cursors();

                } else {

                    uint64_t next_list = pivot;
                    for (; ordered_cursors[next_list]->docs_enum.docid() == pivot_id; --next_list)
                        ;
                    ordered_cursors[next_list]->docs_enum.next_geq(pivot_id);

                    // bubble down the advanced list
                    for (size_t i = next_list + 1; i < ordered_cursors.size(); ++i) {
                        if (ordered_cursors[i]->docs_enum.docid()
                            <= ordered_cursors[i - 1]->docs_enum.docid()) {
                            std::swap(ordered_cursors[i], ordered_cursors[i - 1]);
                        } else {
                            break;
                        }
                    }
                }

            } else {

                uint64_t next;
                uint64_t next_list = pivot;

                float q_weight = ordered_cursors[next_list]->q_weight;

                for (uint64_t i = 0; i < pivot; i++) {
                    if (ordered_cursors[i]->q_weight > q_weight) {
                        next_list = i;
                        q_weight = ordered_cursors[i]->q_weight;
                    }
                }

                uint64_t next_jump = max_docid;

                if (pivot + 1 < ordered_cursors.size()) {
                    next_jump = ordered_cursors[pivot + 1]->docs_enum.docid();
                }

                for (size_t i = 0; i <= pivot; ++i) {
                    if (ordered_cursors[i]->w.docid() < next_jump)
                        next_jump = std::min(ordered_cursors[i]->w.docid(), next_jump);
                }

                next = next_jump + 1;
                if (pivot + 1 < ordered_cursors.size()) {
                    if (next > ordered_cursors[pivot + 1]->docs_enum.docid()) {
                        next = ordered_cursors[pivot + 1]->docs_enum.docid();
                    }
                }

                if (next <= ordered_cursors[pivot]->docs_enum.docid()) {
                    next = ordered_cursors[pivot]->docs_enum.docid() + 1;
                }

                ordered_cursors[next_list]->docs_enum.next_geq(next);

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

    void clear_topk() { m_topk.clear(); }

    topk_queue const &get_topk() const { return m_topk; }

   private:
    topk_queue m_topk;
};

template <typename Index, typename Wand, typename Scorer>
[[nodiscard]] inline auto block_max_wand_executor(Index const &index,
                                           Wand const &wdata,
                                           Scorer const &scorer,
                                           int k) -> QueryExecutor
{
    return [&](Query query) {
	auto run = block_max_wand_query(k);
        auto cursors = make_block_max_scored_cursors(index, wdata, scorer, query);
        run(gsl::make_span(cursors), index.num_docs());
        return run.topk();
    };
}

template <typename Index, typename Wand, typename TermScorer>
struct block_max_scored_cursor;

#define PISA_BLOCK_MAX_WAND_EXECUTOR(SCORER, INDEX, WAND)                                      \
    extern template QueryExecutor block_max_wand_executor(BOOST_PP_CAT(INDEX, _index) const &, \
                                                          wand_data<WAND> const &,             \
                                                          SCORER<wand_data<WAND>> const &,     \
                                                          int);

#define LOOP_BODY(R, DATA, T)                                                                 \
    PISA_DAAT_BLOCK_MAX_ALGORITHM_EXTERN(block_max_wand_query, bm25, T, wand_data_raw)        \
    PISA_DAAT_BLOCK_MAX_ALGORITHM_EXTERN(block_max_wand_query, dph, T, wand_data_raw)         \
    PISA_DAAT_BLOCK_MAX_ALGORITHM_EXTERN(block_max_wand_query, pl2, T, wand_data_raw)         \
    PISA_DAAT_BLOCK_MAX_ALGORITHM_EXTERN(block_max_wand_query, qld, T, wand_data_raw)         \
    PISA_DAAT_BLOCK_MAX_ALGORITHM_EXTERN(block_max_wand_query, bm25, T, wand_data_compressed) \
    PISA_DAAT_BLOCK_MAX_ALGORITHM_EXTERN(block_max_wand_query, dph, T, wand_data_compressed)  \
    PISA_DAAT_BLOCK_MAX_ALGORITHM_EXTERN(block_max_wand_query, pl2, T, wand_data_compressed)  \
    PISA_DAAT_BLOCK_MAX_ALGORITHM_EXTERN(block_max_wand_query, qld, T, wand_data_compressed)  \
    PISA_BLOCK_MAX_WAND_EXECUTOR(bm25, T, wand_data_raw)                                      \
    PISA_BLOCK_MAX_WAND_EXECUTOR(dph, T, wand_data_raw)                                       \
    PISA_BLOCK_MAX_WAND_EXECUTOR(pl2, T, wand_data_raw)                                       \
    PISA_BLOCK_MAX_WAND_EXECUTOR(qld, T, wand_data_raw)                                       \
    PISA_BLOCK_MAX_WAND_EXECUTOR(bm25, T, wand_data_compressed)                               \
    PISA_BLOCK_MAX_WAND_EXECUTOR(dph, T, wand_data_compressed)                                \
    PISA_BLOCK_MAX_WAND_EXECUTOR(pl2, T, wand_data_compressed)                                \
    PISA_BLOCK_MAX_WAND_EXECUTOR(qld, T, wand_data_compressed)
/**/
BOOST_PP_SEQ_FOR_EACH(LOOP_BODY, _, PISA_INDEX_TYPES);
#undef LOOP_BODY
#undef PISA_BLOCK_MAX_WAND_EXECUTOR

} // namespace pisa
