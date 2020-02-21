#pragma once

#include <fmt/format.h>

#include "topk_queue.hpp"
#include "v1/cursor/for_each.hpp"
#include "v1/inspect_query.hpp"
#include "v1/query.hpp"

namespace pisa::v1 {

template <typename Index, typename Scorer, typename Inspect = void>
auto daat_or(Query const& query,
             Index const& index,
             topk_queue topk,
             Scorer&& scorer,
             Inspect* inspect = nullptr)
{
    std::vector<decltype(index.scored_cursor(0, scorer))> cursors;
    std::transform(query.get_term_ids().begin(),
                   query.get_term_ids().end(),
                   std::back_inserter(cursors),
                   [&](auto term) { return index.scored_cursor(term, scorer); });
    auto cunion = v1::union_merge(
        std::move(cursors), 0.0F, [&](auto& score, auto& cursor, auto /* term_idx */) {
            if constexpr (not std::is_void_v<Inspect>) {
                inspect->posting();
            }
            score += cursor.payload();
            return score;
        });
    v1::for_each(cunion, [&](auto& cursor) {
        if constexpr (not std::is_void_v<Inspect>) {
            inspect->document();
            if (topk.insert(cursor.payload(), cursor.value())) {
                inspect->insert();
            }
        } else {
            topk.insert(cursor.payload(), cursor.value());
        }
    });
    return topk;
}

template <typename Index, typename Scorer>
struct InspectDaatOr : Inspect<Index, Scorer, InspectPostings, InspectDocuments, InspectInserts> {

    InspectDaatOr(Index const& index, Scorer const& scorer)
        : Inspect<Index, Scorer, InspectPostings, InspectDocuments, InspectInserts>(index, scorer)
    {
    }

    void run(Query const& query, Index const& index, Scorer const& scorer, topk_queue topk) override
    {
        daat_or(query, index, std::move(topk), scorer, this);
    }
};

} // namespace pisa::v1
