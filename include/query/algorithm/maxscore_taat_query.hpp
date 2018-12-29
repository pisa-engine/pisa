#pragma once

#include "exhaustive_taat_query.hpp"
#include "topk_queue.hpp"
#include "util/intrinsics.hpp"

namespace pisa {

template <typename Index, typename WandType>
[[nodiscard]] auto max_weights(Index const& index, WandType const &wdata, term_id_vec terms)
{
    // TODO(michal): parametrize scorer_type; didn't do that because this might mean some more
    //               complex refactoring I want to avoid for now.
    using scorer_type         = bm25;
    using cursor_type         = typename Index::document_enumerator;
    using score_function_type = std::function<float(uint64_t, uint64_t)>;

    auto query_term_freqs = query_freqs(terms);
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
        done[i] = true;
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

   public:
    maxscore_taat_query(Index const &index, WandType const &wdata, uint64_t k)
        : m_index(index), m_wdata(wdata), m_k(k), m_topk(k), m_accumulators(index.num_docs()) {}

    uint64_t operator()(term_id_vec terms) {
        auto cws = query::cursors_with_scores(m_index, m_wdata, terms);
        return maxscore_taat(
            std::move(cws.first), std::move(cws.second), max_weights(m_index, m_wdata, terms));
    }

    uint64_t operator()([[maybe_unused]] Index const &, term_id_vec terms) {
        auto cws = query::cursors_with_scores(m_index, m_wdata, terms);
        return maxscore_taat(
            std::move(cws.first), std::move(cws.second), max_weights(m_index, m_wdata, terms));
    }

    template <typename Cursor>
    void traverse_with_lookups(Cursor &cursor, score_function_type score) {
        if constexpr (std::is_same_v<typename Cursor::enumerator_category,
                                     ds2i::block_enumerator_tag>) {
            while (cursor.docid() < m_accumulators.size()) {
                auto const &documents = cursor.document_buffer();
                auto const &freqs     = cursor.frequency_buffer();
                #pragma omp simd
                for (uint32_t idx = 0; idx < documents.size(); ++idx) {
                    accumulator_reference accumulator = m_accumulators[documents[idx]];
                    if (accumulator > 0) {
                        accumulator += score(documents[idx], freqs[idx]);
                    }
                }
                cursor.next_block();
            }
        } else {
            for (; cursor.docid() < m_accumulators.size(); cursor.next()) {
                accumulator_reference accumulator = m_accumulators[cursor.docid()];
                if (accumulator > 0) {
                    accumulator += score(cursor.docid(), cursor.freq());
                }
            }
        }
    }

    // TODO(michal): I think this should be eventually the `operator()`
    template <typename Cursor>
    uint64_t maxscore_taat(std::vector<Cursor>              cursors,
                           std::vector<score_function_type> score_functions,
                           std::vector<float>               max_weights) {
        if (cursors.empty()) {
            m_topk.clear();
            return 0;
        }
        sort_many(
            max_weights, [](auto lhs, auto rhs) { return lhs > rhs; }, cursors, score_functions);

        float nonessential_sum = std::accumulate(max_weights.begin(), max_weights.end(), 0.0);
        m_accumulators.init();
        uint32_t term = 0;
        for (; term < cursors.size(); ++term) {
            m_topk.clear();
            m_accumulators.aggregate(m_topk);
            if (not m_topk.would_enter(nonessential_sum)) {
                break;
            }
            Taat_Traversal::traverse_term(cursors[term], score_functions[term], m_accumulators);
            nonessential_sum -= max_weights[term];
        }

        for (; term < cursors.size(); ++term) {
            traverse_with_lookups(cursors[term], score_functions[term]);
        }

        m_topk.clear();
        m_accumulators.aggregate(m_topk);
        m_topk.finalize();
        return m_topk.topk().size();
    }

    std::vector<std::pair<float, uint64_t>> const &topk() const { return m_topk.topk(); }

   private:
    Index const &          m_index;
    WandType const &       m_wdata;
    int                    m_k;
    topk_queue             m_topk;
    Acc                    m_accumulators;
};

template <typename Acc, typename Index, typename WandType>
[[nodiscard]] auto make_maxscore_taat_query(Index const &index, WandType const &wdata, uint64_t k) {
    return maxscore_taat_query<Index, WandType, Acc>(index, wdata, k);
}

}; // namespace pisa
