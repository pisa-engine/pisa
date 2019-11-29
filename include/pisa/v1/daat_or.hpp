#pragma once

#include <fmt/format.h>

#include "topk_queue.hpp"
#include "v1/cursor/for_each.hpp"
#include "v1/query.hpp"

namespace pisa::v1 {

template <typename Index, typename Scorer>
auto daat_or(Query const& query, Index const& index, topk_queue topk, Scorer&& scorer)
{
    std::vector<decltype(index.scored_cursor(0, scorer))> cursors;
    std::transform(query.get_term_ids().begin(),
                   query.get_term_ids().end(),
                   std::back_inserter(cursors),
                   [&](auto term) { return index.scored_cursor(term, scorer); });
    auto cunion = v1::union_merge(
        std::move(cursors), 0.0F, [](auto& score, auto& cursor, auto /* term_idx */) {
            score += cursor.payload();
            return score;
        });
    v1::for_each(cunion, [&](auto& cursor) { topk.insert(cursor.payload(), cursor.value()); });
    return topk;
}

template <typename Index, typename Scorer>
struct DaatOrAnalyzer {
    DaatOrAnalyzer(Index const& index, Scorer scorer) : m_index(index), m_scorer(std::move(scorer))
    {
        std::cout << fmt::format("documents\tpostings\n");
    }

    void operator()(Query const& query)
    {
        std::vector<decltype(m_index.scored_cursor(0, m_scorer))> cursors;
        std::transform(query.get_term_ids().begin(),
                       query.get_term_ids().end(),
                       std::back_inserter(cursors),
                       [&](auto term) { return m_index.scored_cursor(term, m_scorer); });
        std::size_t postings = 0;
        auto cunion = v1::union_merge(
            std::move(cursors), 0.0F, [&](auto& score, auto& cursor, auto /* term_idx */) {
                postings += 1;
                score += cursor.payload();
                return score;
            });
        std::size_t documents = 0;
        std::size_t inserts = 0;
        topk_queue topk(query.k());
        v1::for_each(cunion, [&](auto& cursor) {
            if (topk.insert(cursor.payload(), cursor.value())) {
                inserts += 1;
            };
            documents += 1;
        });
        std::cout << fmt::format("{}\t{}\t{}\n", documents, postings, inserts);
        m_documents += documents;
        m_postings += postings;
        m_inserts += inserts;
        m_count += 1;
    }

    void summarize() &&
    {
        std::cerr << fmt::format(
            "=== SUMMARY ===\nAverage:\n- documents:\t{}\n- postings:\t{}\n- inserts:\t{}\n",
            static_cast<float>(m_documents) / m_count,
            static_cast<float>(m_postings) / m_count,
            static_cast<float>(m_inserts) / m_count);
    }

   private:
    std::size_t m_documents = 0;
    std::size_t m_postings = 0;
    std::size_t m_inserts = 0;
    std::size_t m_count = 0;
    Index const& m_index;
    Scorer m_scorer;
};

} // namespace pisa::v1
