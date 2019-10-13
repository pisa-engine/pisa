#pragma once

#include <algorithm>
#include <iostream>
#include <unordered_set>
#include <vector>

#include <gsl/gsl_assert>
#include <gsl/span>

#include "cursor/collect.hpp"
#include "cursor/cursor.hpp"
#include "cursor/intersection.hpp"
#include "cursor/union.hpp"
#include "int_iter.hpp"
#include "query/queries.hpp"
#include "topk_queue.hpp"

namespace pisa {

[[nodiscard]] [[gnu::always_inline]] inline auto extract_indices(
    std::bitset<64> const &intersection, std::size_t query_length) -> std::vector<std::uint32_t>
{
    Expects(query_length <= std::numeric_limits<std::uint32_t>::max());
    std::vector<std::uint32_t> term_indices;
    term_indices.reserve(query_length);
    for (std::uint32_t term_id = 0; term_id < query_length; ++term_id) {
        if (intersection.test(term_id)) {
            term_indices.push_back(term_id);
        }
    }
    return term_indices;
}

void remap_intersections(std::vector<std::bitset<64>> &intersections,
                         std::vector<std::optional<std::size_t>> const &mapping)
{
    std::vector<std::bitset<64>> remapped(intersections.size());
    for (auto iidx = 0; iidx < remapped.size(); ++iidx) {
        for (auto original = 0; original < mapping.size(); ++original) {
            if (intersections[iidx].test(original) and mapping[original]) {
                remapped[iidx].set(*mapping[original]);
            }
        }
    }
    auto first_empty = std::stable_partition(
        remapped.begin(), remapped.end(), [](auto const &inter) { return inter.any(); });
    remapped.erase(first_empty, remapped.end());
    intersections.swap(remapped);
}

/// This makes sure that:
/// 1. There is no duplicate terms, so scores are not counted twice.
/// 2. Terms are sorted so that it is compatible with `make_cursors`.
inline void resolve(Query &query, std::vector<std::bitset<64>> &intersections)
{
    auto pair_ord = [](auto const &lhs, auto const &rhs) { return lhs.first < rhs.first; };
    auto pair_eq = [](auto const &lhs, auto const &rhs) { return lhs.first == rhs.first; };

    std::vector<std::pair<std::uint32_t, std::size_t>> pairs(query.terms.size());
    std::transform(
        query.terms.begin(), query.terms.end(), iter(0), pairs.begin(), [](auto term, auto pos) {
            return std::make_pair(term, pos);
        });
    std::stable_sort(pairs.begin(), pairs.end(), pair_ord);
    auto first_erased = std::unique(pairs.begin(), pairs.end(), pair_eq);
    std::vector<std::optional<std::size_t>> mapping(pairs.size(), std::nullopt);
    auto mapped = 0;
    std::for_each(pairs.begin(), first_erased, [&](auto original) {
        mapping[original.second] = std::make_optional(mapped++);
    });
    remap_intersections(intersections, mapping);
    query.terms.clear();
    std::transform(pairs.begin(), first_erased, std::back_inserter(query.terms), [](auto const &p) {
        return p.first;
    });
}

template <typename Index, typename Scorer>
inline auto intersection_query(Index const &index,
                               Query query,
                               std::vector<std::bitset<64>> intersections,
                               Scorer scorer,
                               int k)
{
    using payload_type = std::vector<std::pair<std::size_t, std::uint32_t>>;
    using term_mapping_type = std::vector<std::uint32_t>;
    auto accumulate_freq = [](term_mapping_type id_mapping) {
        return [id_mapping = std::move(id_mapping)](payload_type &acc, auto &cursor, auto idx) {
            acc.emplace_back(id_mapping[idx], cursor.freq());
            return acc;
        };
    };
    using accumulate_type = decltype(accumulate_freq(std::declval<term_mapping_type>()));
    using cursor_type = std::decay_t<decltype(make_cursors(index, query)[0])>;
    using intersect_type =
        CursorIntersection<std::vector<cursor_type>, payload_type, accumulate_type>;

    resolve(query, intersections);

    // First, essential lists must be collected. Here, we use on-the-fly intersections,
    // which serve testing purposes -- it results in the same documents as if we used
    // existing intersections from an index.

    std::vector<intersect_type> essential_intersections;
    essential_intersections.reserve(intersections.size());
    for (auto intersection : intersections) {
        auto term_indices = extract_indices(intersection, query.terms.size());
        payload_type init;
        init.reserve(term_indices.size());
        std::vector<std::uint32_t> term_ids(term_indices.size());
        std::transform(term_indices.begin(), term_indices.end(), term_ids.begin(), [&](auto idx) {
            return query.terms[idx];
        });
        auto cursors = make_cursors(index, Query{{}, term_ids, {}});
        essential_intersections.emplace_back(std::move(cursors),
                                             index.num_docs(),
                                             std::move(init),
                                             accumulate_freq(std::move(term_indices)));
    }
    auto lookup_cursors = make_cursors(index, query);
    auto acc_union = [](payload_type &acc, auto &cursor, auto idx) {
        auto payload = cursor.payload();
        acc.insert(acc.end(), payload.begin(), payload.end());
        return acc;
    };
    payload_type init;
    init.reserve(query.terms.size());
    auto candidates = CursorUnion(
        std::move(essential_intersections), index.num_docs(), std::move(init), acc_union);

    std::vector<typename scorer_traits<Scorer>::term_scorer> term_scorers;
    std::transform(query.terms.begin(),
                   query.terms.end(),
                   std::back_inserter(term_scorers),
                   [&](auto term_id) { return scorer.term_scorer(term_id); });

    TopKQueue topk(k);
    while (candidates.docid() < candidates.sentinel()) {
        auto docid = candidates.docid();
        std::vector<std::uint32_t> frequencies(query.terms.size(), 0);
        auto const &payload = candidates.payload();
        std::for_each(payload.begin(), payload.end(), [&](auto const &p) {
            auto [qidx, frequency] = p;
            frequencies[qidx] = frequency;
        });
        float score = 0.0;
        for (auto qidx = 0; qidx < query.terms.size(); ++qidx) {
            if (frequencies[qidx] == 0) {
                lookup_cursors[qidx].next_geq(docid);
                if (lookup_cursors[qidx].docid() == docid) {
                    score += term_scorers[qidx](docid, lookup_cursors[qidx].freq());
                }
            } else {
                score += term_scorers[qidx](docid, frequencies[qidx]);
            }
        }
        topk.insert(score, docid);
        candidates.next();
    }

    topk.finalize();
    return std::move(topk.topk());
}

} // namespace pisa
