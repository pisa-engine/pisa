#pragma once

#include <algorithm>

#include <range/v3/range/conversion.hpp>
#include <range/v3/view/filter.hpp>
#include <range/v3/view/set_algorithm.hpp>
#include <range/v3/view/transform.hpp>

#include "v1/algorithm.hpp"
#include "v1/cursor/labeled_cursor.hpp"
#include "v1/cursor/reference.hpp"
#include "v1/cursor/transform.hpp"
#include "v1/cursor_accumulator.hpp"
#include "v1/inspect_query.hpp"
#include "v1/query.hpp"
#include "v1/runtime_assert.hpp"
#include "v1/union_lookup_join.hpp"

namespace pisa::v1 {

/// This is a special case of Union-Lookup algorithm that does not use user-defined selections,
/// but rather uses the same way of determining essential list as Maxscore does.
/// The difference is that this algorithm will never update the threshold whereas Maxscore will
/// try to improve the estimate after each accumulated document.
template <typename Index, typename Scorer, typename Inspect = void>
auto maxscore_union_lookup(Query const& query,
                           Index const& index,
                           topk_queue topk,
                           Scorer&& scorer,
                           [[maybe_unused]] Inspect* inspect = nullptr)
{
    using cursor_type = decltype(index.max_scored_cursor(0, scorer));
    using payload_type = decltype(std::declval<cursor_type>().payload());

    auto const& term_ids = query.get_term_ids();
    if (term_ids.empty()) {
        return topk;
    }
    auto threshold = query.get_threshold();
    topk.set_threshold(threshold);

    auto cursors = index.max_scored_cursors(gsl::make_span(term_ids), scorer);
    auto [non_essential, essential] = maxscore_partition(gsl::make_span(cursors), threshold);

    std::vector<cursor_type> essential_cursors;
    std::move(essential.begin(), essential.end(), std::back_inserter(essential_cursors));
    std::vector<cursor_type> lookup_cursors;
    std::move(non_essential.begin(), non_essential.end(), std::back_inserter(lookup_cursors));
    std::reverse(lookup_cursors.begin(), lookup_cursors.end());

    auto joined = join_union_lookup(
        std::move(essential_cursors),
        std::move(lookup_cursors),
        payload_type{},
        accumulators::Add{},
        [&](auto score) { return topk.would_enter(score); },
        inspect);
    v1::for_each(joined, [&](auto&& cursor) {
        if constexpr (not std::is_void_v<Inspect>) {
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
struct InspectMaxScoreUnionLookup : Inspect<Index,
                                            Scorer,
                                            InspectPostings,
                                            InspectDocuments,
                                            InspectLookups,
                                            InspectInserts,
                                            InspectEssential> {

    InspectMaxScoreUnionLookup(Index const& index, Scorer const& scorer)
        : Inspect<Index,
                  Scorer,
                  InspectPostings,
                  InspectDocuments,
                  InspectLookups,
                  InspectInserts,
                  InspectEssential>(index, scorer)
    {
    }

    void run(Query const& query, Index const& index, Scorer const& scorer, topk_queue topk) override
    {
        maxscore_union_lookup(query, index, std::move(topk), scorer, this);
    }
};

} // namespace pisa::v1
