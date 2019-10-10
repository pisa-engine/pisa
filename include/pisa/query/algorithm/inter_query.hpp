#pragma once

#include <algorithm>
#include <iostream>
#include <vector>

#include <gsl/gsl_assert>
#include <gsl/span>

#include "cursor/collect.hpp"
#include "cursor/cursor.hpp"
#include "cursor/intersection.hpp"
#include "cursor/union.hpp"
#include "query/queries.hpp"
#include "topk_queue.hpp"

namespace pisa {

[[nodiscard]] [[gnu::always_inline]] inline auto extract_ids(std::bitset<64> const &intersection,
                                                             std::size_t query_length)
    -> std::vector<std::uint32_t>
{
    Expects(query_length <= std::numeric_limits<std::uint32_t>::max());
    std::vector<std::uint32_t> term_ids;
    term_ids.reserve(query_length);
    for (std::uint32_t term_id = 0; term_id < query_length; ++term_id) {
        if (intersection.test(term_id)) {
            term_ids.push_back(term_id);
        }
    }
    return term_ids;
}

template <typename Index, typename Scorer>
inline auto intersection_query(Index const &index,
                               Query const &query,
                               std::vector<std::bitset<64>> const &intersections,
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

    // First, essential lists must be collected. Here, we use on-the-fly intersections,
    // which serve testing purposes -- it results in the same documents as if we used
    // existing intersections from an index.
    // auto essential = make_cursors(index, query);

    std::vector<intersect_type> essential_intersections;
    essential_intersections.reserve(intersections.size());
    for (auto intersection : intersections) {
        auto term_ids = extract_ids(intersection, query.terms.size());
        payload_type init;
        init.reserve(term_ids.size());
        auto cursors = make_cursors(index, Query{{}, term_ids, {}});
        essential_intersections.emplace_back(std::move(cursors),
                                             index.num_docs(),
                                             std::move(init),
                                             accumulate_freq(std::move(term_ids)));
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
