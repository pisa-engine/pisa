#pragma once

#include "util/intrinsics.hpp"
#include "topk_queue.hpp"

#include "accumulator/simple_accumulator.hpp"
#include "accumulator/lazy_accumulator.hpp"
#include "accumulator/blocked_accumulator.hpp"

namespace pisa {

struct Taat_Traversal {
    template <typename Cursor, typename Acc, typename Score>
    void static traverse_term(Cursor &cursor, Score score, Acc &acc)
    {
        if constexpr (std::is_same_v<typename Cursor::enumerator_category,
                                     pisa::block_enumerator_tag>) {
            while (cursor.docid() < acc.size()) {
                auto const &documents = cursor.document_buffer();
                auto const &freqs     = cursor.frequency_buffer();
                for (uint32_t idx = 0; idx < documents.size(); ++idx) {
                    acc.accumulate(documents[idx], score(documents[idx], freqs[idx] + 1));
                }
                cursor.next_block();
            }
        } else {
            for (; cursor.docid() < acc.size(); cursor.next()) {
                acc.accumulate(cursor.docid(), score(cursor.docid(), cursor.freq()));
            }
        }
    }
};

template <typename Index, typename WandType, typename Acc = Simple_Accumulator>
class exhaustive_taat_query {
    using score_function_type = Score_Function<bm25, WandType>;

   public:
    exhaustive_taat_query(Index const &index, WandType const &wdata, uint64_t k)
        : m_index(index), m_wdata(wdata), m_topk(k), m_accumulators(index.num_docs()) {}

    uint64_t operator()(term_id_vec terms) {
        auto [cursors, score_functions] = query::cursors_with_scores(m_index, m_wdata, terms);
        m_topk.clear();
        if (cursors.empty()) {
            return 0;
        }
        m_accumulators.init();
        for (uint32_t term = 0; term < cursors.size(); ++term) {
            Taat_Traversal::traverse_term(cursors[term], score_functions[term], m_accumulators);
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
[[nodiscard]] auto make_exhaustive_taat_query(Index const &   index,
                                              WandType const &wdata,
                                              uint64_t        k) {
    return exhaustive_taat_query<Index, WandType, Acc>(index, wdata, k);
}

}; // namespace pisa
