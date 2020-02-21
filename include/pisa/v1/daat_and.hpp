#pragma once

#include "topk_queue.hpp"
#include "v1/cursor/for_each.hpp"
#include "v1/query.hpp"

namespace pisa::v1 {

template <typename Index, typename Scorer>
auto daat_and(Query const& query, Index const& index, topk_queue topk, Scorer&& scorer)
{
    auto const& term_ids = query.get_term_ids();
    std::vector<decltype(index.scored_cursor(0, scorer))> cursors;
    std::transform(term_ids.begin(), term_ids.end(), std::back_inserter(cursors), [&](auto term) {
        return index.scored_cursor(term, scorer);
    });
    auto intersection =
        v1::intersect(std::move(cursors), 0.0F, [](auto& score, auto& cursor, auto /* term_idx */) {
            score += cursor.payload();
            return score;
        });
    v1::for_each(intersection, [&](auto& cursor) { topk.insert(cursor.payload(), *cursor); });
    return topk;
}

} // namespace pisa::v1
