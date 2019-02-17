#pragma once

#include "util/intrinsics.hpp"
#include "scorer/bm25.hpp"
#include "topk_queue.hpp"
#include "query/queries.hpp"

#include "accumulator/simple_accumulator.hpp"

namespace pisa {

template <typename Index, typename WandType, typename Acc = Simple_Accumulator>
class ranked_or_taat_query {
    using score_function_type = Score_Function<bm25, WandType>;

   public:
    ranked_or_taat_query(Index const &index, WandType const &wdata, uint64_t k, uint64_t max_docid)
        : m_index(index), m_wdata(wdata), m_topk(k), m_max_docid(max_docid), m_accumulator(max_docid) {}

    uint64_t operator()(term_id_vec terms) {
        auto [cursors, score_functions] = query::cursors_with_scores(m_index, m_wdata, terms);
        m_topk.clear();
        if (cursors.empty()) {
            return 0;
        }
        m_accumulator.init();
        for (uint32_t term = 0; term < cursors.size(); ++term) {
            auto cursor = cursors[term];
            const auto score = score_functions[term];
            for (; cursor.docid() < m_max_docid; cursor.next()) {
                m_accumulator.accumulate(cursor.docid(), score(cursor.docid(), cursor.freq()));
            }
        }
        m_accumulator.aggregate(m_topk);
        m_topk.finalize();
        return m_topk.topk().size();
    }

    std::vector<std::pair<float, uint64_t>> const &topk() const { return m_topk.topk(); }

   private:
    Index const &          m_index;
    WandType const &       m_wdata;
    topk_queue             m_topk;
    uint64_t               m_max_docid;
    Acc                    m_accumulator;
};


}; // namespace pisa
