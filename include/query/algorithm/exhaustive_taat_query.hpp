#pragma once

#include "util/intrinsics.hpp"
#include "topk_queue.hpp"

namespace pisa {

// TODO: These are functions common to query processing in general.
//       They should be moved out of this file.
namespace query {

template <typename Cursor>
struct Scored_Cursor {
    Cursor                                   unscored_cursor;
    std::function<float(uint64_t, uint64_t)> score_function;

    [[gnu::always_inline]] [[nodiscard]] constexpr auto docid() const -> uint64_t {
        return unscored_cursor.docid();
    }

    [[gnu::always_inline]] [[nodiscard]] constexpr auto freq() -> uint64_t {
        return unscored_cursor.freq();
    }

    [[gnu::always_inline]] [[nodiscard]] constexpr auto score() -> float {
        return score_function(docid(), freq());
    }

    [[gnu::always_inline]] constexpr void next() { unscored_cursor.next(); }
    [[nodiscard]] auto next_block() -> std::pair<std::vector<uint32_t>, std::vector<float>> {
        auto [documents, frequencies] = unscored_cursor.next_block();
        std::vector<float> scores(documents.size());
        for (uint32_t idx = 0; idx < documents.size(); ++idx) {
            scores[idx] = score_function(documents[idx], frequencies[idx]);
        }
        return std::make_pair(std::move(documents), std::move(scores));
    }
};

template <typename Index, typename WandType>
[[nodiscard]] auto scored_cursors(Index const& index, WandType const &wdata, term_id_vec terms)
{
    // TODO(michal): parametrize scorer_type; didn't do that because this might mean some more
    //               complex refactoring I want to avoid for now.
    using scorer_type = bm25;
    using cursor_type = Scored_Cursor<typename Index::document_enumerator>;

    auto query_term_freqs = query_freqs(terms);
    std::vector<cursor_type> cursors;
    cursors.reserve(query_term_freqs.size());

    for (auto term : query_term_freqs) {
        auto     list     = index[term.first];
        uint64_t num_docs = index.num_docs();
        auto     q_weight = scorer_type::query_term_weight(term.second, list.size(), num_docs);
        cursors.push_back({std::move(list), [q_weight, &wdata](auto docid, auto freq) {
                               float norm_len = wdata.norm_len(docid);
                               return q_weight * scorer_type::doc_term_weight(freq, norm_len);
                           }});
    }
    return cursors;
}

} // namespace query

template <typename Index, typename WandType>
struct exhaustive_taat_query {
    exhaustive_taat_query(Index const &index, WandType const &wdata, uint64_t k)
        : m_index(index), m_wdata(wdata), m_topk(k), m_accumulators(index.num_docs()) {}

    uint64_t operator()(term_id_vec terms) {
        return taat(query::scored_cursors(m_index, m_wdata, terms));
    }

    // TODO(michal): I think this should be eventually the `operator()`
    template <typename Cursor>
    uint64_t taat(std::vector<Cursor> cursors) {
        m_topk.clear();
        if (cursors.empty()) {
            return 0;
        }
        std::fill(m_accumulators.begin(), m_accumulators.end(), 0.0);
        for (auto &cursor : cursors) {
            while (cursor.docid() < m_accumulators.size()) {
                auto [documents, scores] = cursor.next_block();
                for (uint32_t idx = 0; idx < documents.size(); ++idx) {
                    intrinsics::prefetch(&m_accumulators[documents[idx + 3]]);
                    m_accumulators[documents[idx]] += scores[idx];
                }
            }
        }
        for (uint64_t docid = 0u; docid < m_accumulators.size(); ++docid) {
            m_topk.insert(m_accumulators[docid], docid);
        }

        m_topk.finalize();
        return m_topk.topk().size();
    }

    std::vector<std::pair<float, uint64_t>> const &topk() const { return m_topk.topk(); }

   private:
    Index const &      m_index;
    WandType const &   m_wdata;
    topk_queue         m_topk;
    std::vector<float> m_accumulators;
};

template <typename Index, typename WandType>
[[nodiscard]] auto make_exhaustive_taat_query(Index const &   index,
                                              WandType const &wdata,
                                              uint64_t        k) {
    return exhaustive_taat_query<Index, WandType>(index, wdata, k);
}

}; // namespace pisa
