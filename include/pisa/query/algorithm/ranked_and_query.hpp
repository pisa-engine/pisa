#pragma once

#include <vector>

#include <gsl/span>

#include "macro.hpp"
#include "query/queries.hpp"

namespace pisa {

struct ranked_and_query {

    ranked_and_query(uint64_t k) : m_topk(k) {}

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

        // sort by increasing frequency
        std::sort(ordered_cursors.begin(), ordered_cursors.end(), [](Cursor *lhs, Cursor *rhs) {
            return lhs->docs_enum.size() < rhs->docs_enum.size();
        });

        uint64_t candidate = ordered_cursors[0]->docs_enum.docid();
        size_t i = 1;
        while (candidate < max_docid) {
            for (; i < ordered_cursors.size(); ++i) {
                ordered_cursors[i]->docs_enum.next_geq(candidate);
                if (ordered_cursors[i]->docs_enum.docid() != candidate) {
                    candidate = ordered_cursors[i]->docs_enum.docid();
                    i = 0;
                    break;
                }
            }

            if (i == ordered_cursors.size()) {
                float score = 0;
                for (i = 0; i < ordered_cursors.size(); ++i) {
                    score += ordered_cursors[i]->scorer(ordered_cursors[i]->docs_enum.docid(),
                                                        ordered_cursors[i]->docs_enum.freq());
                }

                m_topk.insert(score, ordered_cursors[0]->docs_enum.docid());
                ordered_cursors[0]->docs_enum.next();
                candidate = ordered_cursors[0]->docs_enum.docid();
                i = 1;
            }
        }

        m_topk.finalize();
        return m_topk.topk().size();
    }

    std::vector<std::pair<float, uint64_t>> const &topk() const { return m_topk.topk(); }

    topk_queue &get_topk() { return m_topk; }

   private:
    topk_queue m_topk;
};

template <typename Index, typename TermScorer>
struct scored_cursor;

#define LOOP_BODY(R, DATA, T)                                                   \
    PISA_DAAT_ALGORITHM_EXTERN(ranked_and_query, bm25, T, wand_data_raw)        \
    PISA_DAAT_ALGORITHM_EXTERN(ranked_and_query, dph, T, wand_data_raw)         \
    PISA_DAAT_ALGORITHM_EXTERN(ranked_and_query, pl2, T, wand_data_raw)         \
    PISA_DAAT_ALGORITHM_EXTERN(ranked_and_query, qld, T, wand_data_raw)         \
    PISA_DAAT_ALGORITHM_EXTERN(ranked_and_query, bm25, T, wand_data_compressed) \
    PISA_DAAT_ALGORITHM_EXTERN(ranked_and_query, dph, T, wand_data_compressed)  \
    PISA_DAAT_ALGORITHM_EXTERN(ranked_and_query, pl2, T, wand_data_compressed)  \
    PISA_DAAT_ALGORITHM_EXTERN(ranked_and_query, qld, T, wand_data_compressed)  \
/**/
BOOST_PP_SEQ_FOR_EACH(LOOP_BODY, _, PISA_INDEX_TYPES);
#undef LOOP_BODY

} // namespace pisa
