#pragma once

#include "util/intrinsics.hpp"
#include "scorer/bm25.hpp"
#include "topk_queue.hpp"
#include "query/queries.hpp"

#include "accumulator/simple_accumulator.hpp"

namespace pisa {

template <typename Index, typename WandType, typename Acc = Simple_Accumulator>
class ranked_or_taat_query {
    using scorer_type = bm25;

   public:
    ranked_or_taat_query(Index const &index, WandType const &wdata, uint64_t k, uint64_t max_docid)
        : m_index(index), m_wdata(wdata), m_topk(k), m_max_docid(max_docid), m_accumulator(max_docid) {}

    template <typename Cursor>
    uint64_t operator()(std::vector<Cursor> &&cursors) {
        m_topk.clear();
        if (cursors.empty()) {
            return 0;
        }
        m_accumulator.init();

        auto score  = [&](Cursor &c){
            float    norm_len = m_wdata.norm_len(c.docs_enum.docid());
            return c.q_weight * scorer_type::doc_term_weight(c.docs_enum.freq(), norm_len);

        };
        for (uint32_t term = 0; term < cursors.size(); ++term) {
            auto &cursor = cursors[term];
            for (; cursor.docs_enum.docid() < m_max_docid; cursor.docs_enum.next()) {
                m_accumulator.accumulate(cursor.docs_enum.docid(), score(cursor));
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
