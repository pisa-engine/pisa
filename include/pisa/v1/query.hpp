#pragma once

#include <cstdint>
#include <functional>
#include <vector>

#include <range/v3/action/unique.hpp>
#include <range/v3/algorithm/sort.hpp>
#include <tl/optional.hpp>

#include "topk_queue.hpp"
#include "v1/analyze_query.hpp"
#include "v1/cursor/for_each.hpp"
#include "v1/cursor_intersection.hpp"
#include "v1/cursor_union.hpp"
#include "v1/types.hpp"

namespace pisa::v1 {

struct ListSelection {
    std::vector<std::size_t> unigrams{};
    std::vector<std::pair<std::size_t, std::size_t>> bigrams{};
};

struct Query {
    std::vector<TermId> terms;
    tl::optional<ListSelection> list_selection{};
    tl::optional<float> threshold{};
    tl::optional<std::string> id{};
    int k{};
};

template <typename Index, typename Scorer>
auto daat_and(Query const& query, Index const& index, topk_queue topk, Scorer&& scorer)
{
    std::vector<decltype(index.scored_cursor(0, scorer))> cursors;
    std::transform(query.terms.begin(),
                   query.terms.end(),
                   std::back_inserter(cursors),
                   [&](auto term) { return index.scored_cursor(term, scorer); });
    auto intersection =
        v1::intersect(std::move(cursors), 0.0F, [](auto& score, auto& cursor, auto /* term_idx */) {
            score += cursor.payload();
            return score;
        });
    v1::for_each(intersection, [&](auto& cursor) { topk.insert(cursor.payload(), *cursor); });
    return topk;
}

template <typename Index, typename Scorer>
auto daat_or(Query const& query, Index const& index, topk_queue topk, Scorer&& scorer)
{
    std::vector<decltype(index.scored_cursor(0, scorer))> cursors;
    std::transform(query.terms.begin(),
                   query.terms.end(),
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
        std::transform(query.terms.begin(),
                       query.terms.end(),
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
        topk_queue topk(query.k);
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

template <typename Index, typename Scorer>
auto taat_or(Query const& query, Index const& index, topk_queue topk, Scorer&& scorer)
{
    std::vector<float> accumulator(index.num_documents(), 0.0F);
    for (auto term : query.terms) {
        v1::for_each(index.scored_cursor(term, scorer),
                     [&accumulator](auto&& cursor) { accumulator[*cursor] += cursor.payload(); });
    }
    for (auto document = 0; document < accumulator.size(); document += 1) {
        topk.insert(accumulator[document], document);
    }
    return topk;
}

/// Returns only unique terms, in sorted order.
[[nodiscard]] auto filter_unique_terms(Query const& query) -> std::vector<TermId>;

template <typename Container>
auto transform()
{
}

} // namespace pisa::v1
