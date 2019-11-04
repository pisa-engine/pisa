#pragma once

#include <cstdint>
#include <functional>

#include "topk_queue.hpp"
#include "v1/cursor/for_each.hpp"
#include "v1/cursor_intersection.hpp"
#include "v1/cursor_union.hpp"
#include "v1/types.hpp"

namespace pisa::v1 {

struct Query {
    std::vector<TermId> terms;
};

template <typename Index>
using QueryProcessor = std::function<topk_queue(Index const &, Query const &, topk_queue)>;

struct ExhaustiveConjunctiveProcessor {
    template <typename Index>
    auto operator()(Index const &index, Query const &query, topk_queue que) -> topk_queue
    {
        using Cursor = std::decay_t<decltype(index.cusror(0))>;
        std::vector<Cursor> cursors;
        std::transform(query.terms.begin(),
                       query.terms.end(),
                       std::back_inserter(cursors),
                       [&index](auto term_id) { return index.cursor(term_id); });
        auto intersection =
            intersect(std::move(cursors),
                      0.0F,
                      [](float score, auto &cursor, [[maybe_unused]] auto cursor_idx) {
                          return score + static_cast<float>(cursor.payload());
                      });
        while (not intersection.empty()) {
            que.insert(intersection.payload(), *intersection);
        }
        return que;
    }
};

template <typename Index, typename Scorer>
auto daat_and(Query const &query, Index const &index, topk_queue topk, Scorer &&scorer)
{
    std::vector<decltype(index.scored_cursor(0, scorer))> cursors;
    std::transform(query.terms.begin(),
                   query.terms.end(),
                   std::back_inserter(cursors),
                   [&](auto term) { return index.scored_cursor(term, scorer); });
    auto intersection =
        v1::intersect(std::move(cursors), 0.0F, [](auto &score, auto &cursor, auto /* term_idx */) {
            score += cursor.payload();
            return score;
        });
    v1::for_each(intersection, [&](auto &cursor) { topk.insert(cursor.payload(), *cursor); });
    return topk;
}

template <typename Index, typename Scorer>
auto daat_or(Query const &query, Index const &index, topk_queue topk, Scorer &&scorer)
{
    std::vector<decltype(index.scored_cursor(0, scorer))> cursors;
    std::transform(query.terms.begin(),
                   query.terms.end(),
                   std::back_inserter(cursors),
                   [&](auto term) { return index.scored_cursor(term, scorer); });
    auto cunion = v1::union_merge(
        std::move(cursors), 0.0F, [](auto &score, auto &cursor, auto /* term_idx */) {
            score += cursor.payload();
            return score;
        });
    v1::for_each(cunion, [&](auto &cursor) { topk.insert(cursor.payload(), cursor.value()); });
    return topk;
}

template <typename Index, typename Scorer>
auto taat_or(Query const &query, Index const &index, topk_queue topk, Scorer &&scorer)
{
    std::vector<float> accumulator(index.num_documents(), 0.0F);
    for (auto term : query.terms) {
        v1::for_each(index.scored_cursor(term, scorer),
                     [&accumulator](auto &&cursor) { accumulator[*cursor] += cursor.payload(); });
    }
    for (auto document = 0; document < accumulator.size(); document += 1) {
        topk.insert(accumulator[document], document);
    }
    return topk;
}

} // namespace pisa::v1
