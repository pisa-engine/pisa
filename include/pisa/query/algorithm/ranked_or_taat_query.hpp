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
    ranked_or_taat_query(Index const &index, WandType const &wdata, uint64_t k)
        : m_index(index), m_wdata(wdata), m_topk(k), m_accumulators(index.num_docs()) {}

    uint64_t operator()(term_id_vec terms) {
        auto [cursors, score_functions] = query::cursors_with_scores(m_index, m_wdata, terms);
        m_topk.clear();
        if (cursors.empty()) {
            return 0;
        }
        m_accumulators.init();
        for (uint32_t term = 0; term < cursors.size(); ++term) {
            auto cursor = cursors[term];
            const auto score = score_functions[term];
            for (; cursor.docid() < m_accumulators.size(); cursor.next()) {
                m_accumulators.accumulate(cursor.docid(), score(cursor.docid(), cursor.freq()));
            }
        }
        m_accumulators.aggregate(m_topk);
        m_topk.finalize();
        return m_topk.topk().size();
    }

    std::vector<std::pair<float, uint64_t>> const &topk() const { return m_topk.topk(); }

   private:
    Index const &          m_index;
    WandType const &       m_wdata;
    topk_queue             m_topk;
    Acc                    m_accumulators;
};

template <typename Acc, typename Index, typename WandType>
[[nodiscard]] auto make_ranked_or_taat_query(Index const &   index,
                                              WandType const &wdata,
                                              uint64_t        k) {
    return ranked_or_taat_query<Index, WandType, Acc>(index, wdata, k);
}

}; // namespace pisa
