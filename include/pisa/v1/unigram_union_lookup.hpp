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

/// Processes documents with the Union-Lookup method.
/// This is an optimized version that works **only on single-term posting lists**.
/// It will throw an exception if bigram selections are passed to it.
template <typename Index, typename Scorer, typename Inspect = void>
auto unigram_union_lookup(Query const& query,
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

    auto const& selections = query.get_selections();
    runtime_assert(selections.bigrams.empty()).or_throw("This algorithm only supports unigrams");

    topk.set_threshold(query.get_threshold());

    auto non_essential_terms =
        ranges::views::set_difference(term_ids, selections.unigrams) | ranges::to_vector;

    auto essential_cursors = index.max_scored_cursors(selections.unigrams, scorer);
    auto lookup_cursors = index.max_scored_cursors(non_essential_terms, scorer);
    ranges::sort(lookup_cursors, [](auto&& l, auto&& r) { return l.max_score() > r.max_score(); });

    if constexpr (not std::is_void_v<Inspect>) {
        inspect->essential(essential_cursors.size());
    }

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
struct InspectUnigramUnionLookup : Inspect<Index,
                                           Scorer,
                                           InspectPostings,
                                           InspectDocuments,
                                           InspectLookups,
                                           InspectInserts,
                                           InspectEssential> {

    InspectUnigramUnionLookup(Index const& index, Scorer const& scorer)
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
        unigram_union_lookup(query, index, std::move(topk), scorer, this);
    }
};

} // namespace pisa::v1
