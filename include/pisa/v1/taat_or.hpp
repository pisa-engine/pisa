#pragma once

#include <fmt/format.h>

#include "topk_queue.hpp"
#include "v1/cursor/for_each.hpp"
#include "v1/query.hpp"

namespace pisa::v1 {

template <typename Index, typename Scorer>
auto taat_or(Query const& query, Index const& index, topk_queue topk, Scorer&& scorer)
{
    std::vector<float> accumulator(index.num_documents(), 0.0F);
    for (auto term : query.get_term_ids()) {
        v1::for_each(index.scored_cursor(term, scorer),
                     [&accumulator](auto&& cursor) { accumulator[*cursor] += cursor.payload(); });
    }
    for (auto document = 0; document < accumulator.size(); document += 1) {
        topk.insert(accumulator[document], document);
    }
    return topk;
}

} // namespace pisa::v1
