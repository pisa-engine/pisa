#pragma once

#include "topk_queue.hpp"
#include "util/intrinsics.hpp"

#include "accumulator/blocked_accumulator.hpp"
#include "accumulator/lazy_accumulator.hpp"
#include "accumulator/simple_accumulator.hpp"

namespace pisa {

template <typename Index, typename WandType>
[[nodiscard]] auto max_weights(Index const &index, WandType const &wdata, term_id_vec terms) {
    // TODO(michal): parametrize scorer_type; didn't do that because this might mean some more
    //               complex refactoring I want to avoid for now.
    using scorer_type         = bm25;
    using cursor_type         = typename Index::document_enumerator;
    using score_function_type = Score_Function<scorer_type, WandType>;

    auto               query_term_freqs = query_freqs(terms);
    std::vector<float> max_weights;
    max_weights.reserve(query_term_freqs.size());

    for (auto term : query_term_freqs) {
        auto list     = index[term.first];
        auto q_weight = scorer_type::query_term_weight(term.second, list.size(), index.num_docs());
        max_weights.push_back(q_weight * wdata.max_term_weight(term.first));
    }
    return max_weights;
}

template <typename Container, typename Function>
std::vector<std::size_t> sort_permutation(Container const &container, Function sort_function) {
    std::vector<std::size_t> p(container.size());
    std::iota(p.begin(), p.end(), 0);
    std::sort(p.begin(), p.end(), [&](std::size_t i, std::size_t j) {
        return sort_function(container[i], container[j]);
    });
    return p;
}

template <typename Container>
void apply_permutation(Container &container, const std::vector<std::size_t> &p) {
    std::vector<bool> done(container.size());
    for (std::size_t i = 0; i < container.size(); ++i) {
        if (done[i]) {
            continue;
        }
        done[i]            = true;
        std::size_t prev_j = i;
        std::size_t j      = p[i];
        while (i != j) {
            std::swap(container[prev_j], container[j]);
            done[j] = true;
            prev_j  = j;
            j       = p[j];
        }
    }
}

template <typename Container, typename... Containers, typename Function>
void sort_many(Container &key_container, Function sort_function, Containers &... containers) {
    auto permutation = sort_permutation(key_container, sort_function);
    (apply_permutation(containers, permutation), ...);
}

template <typename Index, typename WandType, typename Acc = Simple_Accumulator>
class maxscore_taat_query {
    using accumulator_reference = typename Acc::reference;
    using score_function_type   = Score_Function<bm25, WandType>;

   public:
    maxscore_taat_query(Index const &index, WandType const &wdata, uint64_t k)
        : m_index(index), m_wdata(wdata), m_k(k), m_topk(k), m_accumulators(index.num_docs()) {}

    uint64_t operator()(term_id_vec terms) {
        m_topk.clear();
        auto cws = query::cursors_with_scores(m_index, m_wdata, terms);
        auto cursors = cws.first;
        auto score_functions = cws.second;
        auto m_w = max_weights(m_index, m_wdata, terms);
        if (cursors.empty()) {
            return 0;
        }
        sort_many(
            m_w, [](auto lhs, auto rhs) { return lhs > rhs; }, cursors, score_functions);

        float nonessential_sum = std::accumulate(m_w.begin(), m_w.end(), 0.0);
        m_accumulators.init();
        uint32_t term = 0;
        for (; term < cursors.size(); ++term) {
            if (not m_topk.would_enter(nonessential_sum)) {
                break;
            }
            m_topk.clear();
            auto cursor = cursors[term];
            auto score = score_functions[term];
            // TODO(antonio): basically here we can do a bit better.
            // before scoring a document, we read its accumulator value and check if the sum of
            // the accumulator value and the upper bound of the maxscores of the missing terms
            // (current included) is greater than the threshold. If it is we score and add it to the
            // accumulator, we go to the next document otherwise.
            for (; cursor.docid() < m_accumulators.size(); cursor.next()) {
                if(m_topk.would_enter(nonessential_sum + m_accumulators[cursor.docid()])) {
                    m_accumulators.accumulate(cursor.docid(), score(cursor.docid(), cursor.freq()));
                    m_topk.insert(m_accumulators[cursor.docid()]);
                }
            }
            nonessential_sum -= m_w[term];
        }

        for (; term < cursors.size(); ++term) {
            auto cursor = cursors[term];
            auto score = score_functions[term];
            for (; cursor.docid() < m_accumulators.size(); cursor.next()) {
                accumulator_reference accumulator = m_accumulators[cursor.docid()];
                if (accumulator > 0) {
                    accumulator += score(cursor.docid(), cursor.freq());
                }
            }
        }

        m_topk.clear();
        m_accumulators.aggregate(m_topk);
        m_topk.finalize();
        return m_topk.topk().size();
    }


    std::vector<std::pair<float, uint64_t>> const &topk() const { return m_topk.topk(); }

   private:
    Index const &   m_index;
    WandType const &m_wdata;
    int             m_k;
    topk_queue      m_topk;
    Acc             m_accumulators;
};

template <typename Acc, typename Index, typename WandType>
[[nodiscard]] auto make_maxscore_taat_query(Index const &index, WandType const &wdata, uint64_t k) {
    return maxscore_taat_query<Index, WandType, Acc>(index, wdata, k);
}

}; // namespace pisa
