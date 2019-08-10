#pragma once

#include <vector>

#include <gsl/span>

#include "cursor/block_max_scored_cursor.hpp"
#include "macro.hpp"
#include "query/queries.hpp"
#include "topk_queue.hpp"

namespace pisa {

struct block_max_maxscore_query {

    block_max_maxscore_query(uint64_t k) : m_topk(k) {}

    template <typename Cursor>
    uint64_t operator()(gsl::span<Cursor> cursors, uint64_t max_docid)
    {
        m_topk.clear();
        if (cursors.empty()) {
            return 0;
        }

        std::vector<Cursor *> ordered_cursors;
        ordered_cursors.reserve(cursors.size());
        for (auto &en : cursors) {
            ordered_cursors.push_back(&en);
        }

        // sort enumerators by increasing maxscore
        std::sort(ordered_cursors.begin(), ordered_cursors.end(), [](Cursor *lhs, Cursor *rhs) {
            return lhs->max_weight < rhs->max_weight;
        });

        std::vector<float> upper_bounds(ordered_cursors.size());
        upper_bounds[0] = ordered_cursors[0]->max_weight;
        for (size_t i = 1; i < ordered_cursors.size(); ++i) {
            upper_bounds[i] = upper_bounds[i - 1] + ordered_cursors[i]->max_weight;
        }

        int non_essential_lists = 0;
        uint64_t cur_doc =
            std::min_element(cursors.begin(),
                             cursors.end(),
                             [](Cursor const &lhs, Cursor const &rhs) {
                                 return lhs.docs_enum.docid() < rhs.docs_enum.docid();
                             })
                ->docs_enum.docid();

        while (non_essential_lists < ordered_cursors.size() && cur_doc < max_docid) {
            float score = 0;
            uint64_t next_doc = max_docid;
            for (size_t i = non_essential_lists; i < ordered_cursors.size(); ++i) {
                if (ordered_cursors[i]->docs_enum.docid() == cur_doc) {
                    score += ordered_cursors[i]->scorer(ordered_cursors[i]->docs_enum.docid(),
                                                        ordered_cursors[i]->docs_enum.freq());
                    ordered_cursors[i]->docs_enum.next();
                }
                if (ordered_cursors[i]->docs_enum.docid() < next_doc) {
                    next_doc = ordered_cursors[i]->docs_enum.docid();
                }
            }

            double block_upper_bound =
                non_essential_lists > 0 ? upper_bounds[non_essential_lists - 1] : 0;
            for (int i = non_essential_lists - 1; i + 1 > 0; --i) {
                if (ordered_cursors[i]->w.docid() < cur_doc) {
                    ordered_cursors[i]->w.next_geq(cur_doc);
                }
                block_upper_bound -= ordered_cursors[i]->max_weight
                                     - ordered_cursors[i]->w.score() * ordered_cursors[i]->q_weight;
                if (!m_topk.would_enter(score + block_upper_bound)) {
                    break;
                }
            }
            if (m_topk.would_enter(score + block_upper_bound)) {
                // try to complete evaluation with non-essential lists
                for (size_t i = non_essential_lists - 1; i + 1 > 0; --i) {
                    ordered_cursors[i]->docs_enum.next_geq(cur_doc);
                    if (ordered_cursors[i]->docs_enum.docid() == cur_doc) {
                        auto s = ordered_cursors[i]->scorer(ordered_cursors[i]->docs_enum.docid(),
                                                            ordered_cursors[i]->docs_enum.freq());
                        // score += s;
                        block_upper_bound += s;
                    }
                    block_upper_bound -=
                        ordered_cursors[i]->w.score() * ordered_cursors[i]->q_weight;

                    if (!m_topk.would_enter(score + block_upper_bound)) {
                        break;
                    }
                }
                score += block_upper_bound;
            }
            if (m_topk.insert(score, cur_doc)) {
                // update non-essential lists
                while (non_essential_lists < ordered_cursors.size()
                       && !m_topk.would_enter(upper_bounds[non_essential_lists])) {
                    non_essential_lists += 1;
                }
            }
            cur_doc = next_doc;
        }
        m_topk.finalize();
        return m_topk.topk().size();
    }

    std::vector<std::pair<float, uint64_t>> const &topk() const { return m_topk.topk(); }

   private:
    topk_queue m_topk;
};

template <typename Index, typename Wand, typename TermScorer>
struct block_max_scored_cursor;

#define LOOP_BODY(R, DATA, T)                                                                     \
    PISA_DAAT_BLOCK_MAX_ALGORITHM_EXTERN(block_max_maxscore_query, bm25, T, wand_data_raw)        \
    PISA_DAAT_BLOCK_MAX_ALGORITHM_EXTERN(block_max_maxscore_query, dph, T, wand_data_raw)         \
    PISA_DAAT_BLOCK_MAX_ALGORITHM_EXTERN(block_max_maxscore_query, pl2, T, wand_data_raw)         \
    PISA_DAAT_BLOCK_MAX_ALGORITHM_EXTERN(block_max_maxscore_query, qld, T, wand_data_raw)         \
    PISA_DAAT_BLOCK_MAX_ALGORITHM_EXTERN(block_max_maxscore_query, bm25, T, wand_data_compressed) \
    PISA_DAAT_BLOCK_MAX_ALGORITHM_EXTERN(block_max_maxscore_query, dph, T, wand_data_compressed)  \
    PISA_DAAT_BLOCK_MAX_ALGORITHM_EXTERN(block_max_maxscore_query, pl2, T, wand_data_compressed)  \
    PISA_DAAT_BLOCK_MAX_ALGORITHM_EXTERN(block_max_maxscore_query, qld, T, wand_data_compressed)
/**/
BOOST_PP_SEQ_FOR_EACH(LOOP_BODY, _, PISA_INDEX_TYPES);
#undef LOOP_BODY

} // namespace pisa
