#pragma once

#include <gsl/span>

#include "accumulator/lazy_accumulator.hpp"
#include "accumulator/simple_accumulator.hpp"
#include "macro.hpp"
#include "query/queries.hpp"
#include "topk_queue.hpp"
#include "util/intrinsics.hpp"

namespace pisa {

class ranked_or_taat_query {
   public:
    ranked_or_taat_query(uint64_t k) : m_topk(k) {}

    template <typename Cursor, typename Acc>
    uint64_t operator()(gsl::span<Cursor> cursors, uint64_t max_docid, Acc &&accumulator)
    {
        m_topk.clear();
        if (cursors.empty()) {
            return 0;
        }
        accumulator.init();

        for (auto &&cursor : cursors) {
            while (cursor.docs_enum.docid() < max_docid) {
                accumulator.accumulate(
                    cursor.docs_enum.docid(),
                    cursor.scorer(cursor.docs_enum.docid(), cursor.docs_enum.freq()));
                cursor.docs_enum.next();
            }
        }
        accumulator.aggregate(m_topk);
        m_topk.finalize();
        return m_topk.topk().size();
    }

    std::vector<std::pair<float, uint64_t>> const &topk() const { return m_topk.topk(); }

   private:
    TopKQueue m_topk;
};

template <typename Index, typename TermScorer>
struct scored_cursor;

#define LOOP_BODY(R, DATA, T)                                                                    \
    PISA_TAAT_ALGORITHM_EXTERN(ranked_or_taat_query, bm25, T, wand_data_raw, SimpleAccumulator)  \
    PISA_TAAT_ALGORITHM_EXTERN(ranked_or_taat_query, dph, T, wand_data_raw, SimpleAccumulator)   \
    PISA_TAAT_ALGORITHM_EXTERN(ranked_or_taat_query, pl2, T, wand_data_raw, SimpleAccumulator)   \
    PISA_TAAT_ALGORITHM_EXTERN(ranked_or_taat_query, qld, T, wand_data_raw, SimpleAccumulator)   \
    PISA_TAAT_ALGORITHM_EXTERN(                                                                  \
        ranked_or_taat_query, bm25, T, wand_data_compressed, SimpleAccumulator)                  \
    PISA_TAAT_ALGORITHM_EXTERN(                                                                  \
        ranked_or_taat_query, dph, T, wand_data_compressed, SimpleAccumulator)                   \
    PISA_TAAT_ALGORITHM_EXTERN(                                                                  \
        ranked_or_taat_query, pl2, T, wand_data_compressed, SimpleAccumulator)                   \
    PISA_TAAT_ALGORITHM_EXTERN(                                                                  \
        ranked_or_taat_query, qld, T, wand_data_compressed, SimpleAccumulator)                   \
                                                                                                 \
    PISA_TAAT_ALGORITHM_EXTERN(ranked_or_taat_query, bm25, T, wand_data_raw, LazyAccumulator<4>) \
    PISA_TAAT_ALGORITHM_EXTERN(ranked_or_taat_query, dph, T, wand_data_raw, LazyAccumulator<4>)  \
    PISA_TAAT_ALGORITHM_EXTERN(ranked_or_taat_query, pl2, T, wand_data_raw, LazyAccumulator<4>)  \
    PISA_TAAT_ALGORITHM_EXTERN(ranked_or_taat_query, qld, T, wand_data_raw, LazyAccumulator<4>)  \
    PISA_TAAT_ALGORITHM_EXTERN(                                                                  \
        ranked_or_taat_query, bm25, T, wand_data_compressed, LazyAccumulator<4>)                 \
    PISA_TAAT_ALGORITHM_EXTERN(                                                                  \
        ranked_or_taat_query, dph, T, wand_data_compressed, LazyAccumulator<4>)                  \
    PISA_TAAT_ALGORITHM_EXTERN(                                                                  \
        ranked_or_taat_query, pl2, T, wand_data_compressed, LazyAccumulator<4>)                  \
    PISA_TAAT_ALGORITHM_EXTERN(                                                                  \
        ranked_or_taat_query, qld, T, wand_data_compressed, LazyAccumulator<4>)
/**/
BOOST_PP_SEQ_FOR_EACH(LOOP_BODY, _, PISA_INDEX_TYPES);
#undef LOOP_BODY

}; // namespace pisa
