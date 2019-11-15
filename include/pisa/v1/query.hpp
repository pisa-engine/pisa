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

template <typename Index>
using QueryProcessor = std::function<topk_queue(Index const&, Query const&, topk_queue)>;

struct ExhaustiveConjunctiveProcessor {
    template <typename Index>
    auto operator()(Index const& index, Query const& query, topk_queue que) -> topk_queue
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
                      [](float score, auto& cursor, [[maybe_unused]] auto cursor_idx) {
                          return score + static_cast<float>(cursor.payload());
                      });
        while (not intersection.empty()) {
            que.insert(intersection.payload(), *intersection);
        }
        return que;
    }
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

/// Performs a "union-lookup" query (name pending).
///
/// \param  query               Full query, as received, possibly with duplicates.
/// \param  index               Inverted index, with access to both unigrams and bigrams.
/// \param  topk                Top-k heap.
/// \param  scorer              An object capable of constructing term scorers.
/// \param  essential_unigrams  A list of essential single-term posting lists.
///                             Elements of this vector point to the index of the term
///                             in the query. In other words, for each position `i` in this vector,
///                             `query.terms[essential_unigrams[i]]` is an essential unigram.
/// \param  essential_bigrams   Similar to the above, but represents intersections between two
///                             posting lists. These must exist in the index, or else this
///                             algorithm will fail.
template <typename Index, typename Scorer>
auto union_lookup(Query const& query,
                  Index const& index,
                  topk_queue topk,
                  Scorer&& scorer,
                  std::vector<std::size_t> essential_unigrams,
                  std::vector<std::pair<std::size_t, std::size_t>> essential_bigrams)
{
    ranges::sort(essential_unigrams);
    ranges::actions::unique(essential_unigrams);
    ranges::sort(essential_bigrams);
    ranges::actions::unique(essential_bigrams);

    std::vector<float> initial_payload(query.terms.size(), 0.0);

    std::vector<decltype(index.scored_cursor(0, scorer))> essential_unigram_cursors;
    std::transform(essential_unigrams.begin(),
                   essential_unigrams.end(),
                   std::back_inserter(essential_unigram_cursors),
                   [&](auto idx) { return index.scored_cursor(query.terms[idx], scorer); });
    auto merged_unigrams = v1::union_merge(
        essential_unigram_cursors, initial_payload, [&](auto& acc, auto& cursor, auto term_idx) {
            acc[essential_unigrams[term_idx]] = cursor.payload();
            return acc;
        });

    std::vector<decltype(index.scored_bigram_cursor(0, 0, scorer))> essential_bigram_cursors;
    std::transform(essential_bigrams.begin(),
                   essential_bigrams.end(),
                   std::back_inserter(essential_bigram_cursors),
                   [&](auto intersection) {
                       return index.scored_bigram_cursor(query.terms[intersection.first],
                                                         query.terms[intersection.second],
                                                         scorer);
                   });
    auto merged_bigrams =
        v1::union_merge(std::move(essential_bigram_cursors),
                        initial_payload,
                        [&](auto& acc, auto& cursor, auto term_idx) {
                            auto payload = cursor.payload();
                            acc[essential_bigrams[term_idx].first] = std::get<0>(payload);
                            acc[essential_bigrams[term_idx].second] = std::get<1>(payload);
                            return acc;
                        });

    auto accumulate = [&](auto& acc, auto& cursor, auto /* union_idx */) {
        auto payload = cursor.payload();
        for (auto idx = 0; idx < acc.size(); idx += 1) {
            acc[idx] = payload[idx];
        }
        return acc;
    };
    auto merged = v1::variadic_union_merge(
        initial_payload,
        std::make_tuple(std::move(merged_unigrams), std::move(merged_bigrams)),
        std::make_tuple(accumulate, accumulate));

    std::vector<decltype(index.scored_cursor(0, scorer))> lookup_cursors;
    std::transform(query.terms.begin(),
                   query.terms.end(),
                   std::back_inserter(lookup_cursors),
                   [&](auto term) { return index.scored_cursor(term, scorer); });

    v1::for_each(merged, [&](auto& cursor) {
        auto docid = cursor.value();
        auto partial_scores = cursor.payload();
        float score = 0.0F;
        for (auto idx = 0; idx < partial_scores.size(); idx += 1) {
            if (partial_scores[idx] > 0.0F) {
                score += partial_scores[idx];
            } else {
                lookup_cursors[idx].advance_to_geq(docid);
                if (lookup_cursors[idx].value() == docid) {
                    score += lookup_cursors[idx].payload();
                }
            }
        }
        topk.insert(score, docid);
    });
    return topk;
}

} // namespace pisa::v1
