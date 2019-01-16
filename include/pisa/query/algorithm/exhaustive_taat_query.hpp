#pragma once

#include "util/intrinsics.hpp"
#include "topk_queue.hpp"

#include "accumulator/simple_accumulator.hpp"
#include "accumulator/lazy_accumulator.hpp"
#include "accumulator/blocked_accumulator.hpp"

namespace pisa {

template <typename Scorer, typename Wand>
struct Score_Function {
    float query_weight;
    std::reference_wrapper<Wand const> wdata;

    [[nodiscard]] auto operator()(uint32_t doc, uint32_t freq) const -> float {
        return query_weight * Scorer::doc_term_weight(freq, wdata.get().norm_len(doc));
    }
};

// TODO: These are functions common to query processing in general.
//       They should be moved out of this file.
namespace query {

template <typename Index, typename WandType>
[[nodiscard]] auto cursors_with_scores(Index const& index, WandType const &wdata, term_id_vec terms)
{
    // TODO(michal): parametrize scorer_type; didn't do that because this might mean some more
    //               complex refactoring I want to avoid for now.
    using scorer_type         = bm25;
    using cursor_type         = typename Index::document_enumerator;
    using score_function_type = Score_Function<scorer_type, WandType>;

    auto query_term_freqs = query_freqs(terms);
    std::vector<cursor_type> cursors;
    std::vector<score_function_type> score_functions;
    cursors.reserve(query_term_freqs.size());
    score_functions.reserve(query_term_freqs.size());

    for (auto term : query_term_freqs) {
        auto     list     = index[term.first];
        uint64_t num_docs = index.num_docs();
        auto     q_weight = scorer_type::query_term_weight(term.second, list.size(), num_docs);
        cursors.push_back(std::move(list));
        score_functions.push_back({q_weight, std::cref(wdata)});
    }
    return std::make_pair(cursors, score_functions);
}

} // namespace query

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
        auto cws = query::cursors_with_scores(m_index, m_wdata, terms);
        return taat(std::move(cws.first), std::move(cws.second));
    }

    uint64_t operator()([[maybe_unused]] Index const &, term_id_vec terms) {
        auto cws = query::cursors_with_scores(m_index, m_wdata, terms);
        return taat(std::move(cws.first), std::move(cws.second));
    }

    // TODO(michal): I think this should be eventually the `operator()`
    template <typename Cursor>
    uint64_t taat(std::vector<Cursor> cursors, std::vector<score_function_type> score_functions) {
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
