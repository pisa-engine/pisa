#pragma once

#include <cstdint>
#include <functional>

#include "topk_queue.hpp"
#include "v1/cursor_intersection.hpp"
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

} // namespace pisa::v1
