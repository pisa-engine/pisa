#pragma once

#include <cstdint>
#include <functional>

#include <range/v3/action/unique.hpp>
#include <range/v3/algorithm/sort.hpp>

#include "topk_queue.hpp"
#include "v1/cursor/for_each.hpp"
#include "v1/cursor_intersection.hpp"
#include "v1/cursor_union.hpp"
#include "v1/types.hpp"

namespace pisa::v1 {

struct Query {
    std::vector<TermId> terms;
    std::vector<std::pair<TermId, TermId>> bigrams{};
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
