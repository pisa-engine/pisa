#pragma once

#include "v1/query.hpp"

namespace pisa::v1 {

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

    std::vector<bool> is_essential(query.terms.size(), false);
    // std::cerr << "essential: ";
    for (auto idx : essential_unigrams) {
        // std::cerr << idx << ' ';
        is_essential[idx] = true;
    }
    // std::cerr << '\n';

    // std::vector<float> initial_payload(query.terms.size(), 0.0);

    // std::vector<decltype(index.scored_cursor(0, scorer))> essential_unigram_cursors;
    // std::transform(essential_unigrams.begin(),
    //               essential_unigrams.end(),
    //               std::back_inserter(essential_unigram_cursors),
    //               [&](auto idx) { return index.scored_cursor(query.terms[idx], scorer); });
    // auto merged_unigrams = v1::union_merge(
    //    essential_unigram_cursors, initial_payload, [&](auto& acc, auto& cursor, auto term_idx) {
    //        acc[essential_unigrams[term_idx]] = cursor.payload();
    //        return acc;
    //    });

    std::vector<decltype(index.scored_cursor(0, scorer))> essential_unigram_cursors;
    std::transform(essential_unigrams.begin(),
                   essential_unigrams.end(),
                   std::back_inserter(essential_unigram_cursors),
                   [&](auto idx) { return index.scored_cursor(query.terms[idx], scorer); });
    // std::cerr << "No. essential: " << essential_unigram_cursors.size() << '\n';

    auto merged_unigrams = v1::union_merge(
        essential_unigram_cursors, 0.0F, [&](auto acc, auto& cursor, auto /*term_idx*/) {
            // acc[essential_unigrams[term_idx]] = cursor.payload();
            return acc + cursor.payload();
        });

    // std::vector<decltype(index.scored_bigram_cursor(0, 0, scorer))> essential_bigram_cursors;
    // std::transform(essential_bigrams.begin(),
    //               essential_bigrams.end(),
    //               std::back_inserter(essential_bigram_cursors),
    //               [&](auto intersection) {
    //                   return index.scored_bigram_cursor(query.terms[intersection.first],
    //                                                     query.terms[intersection.second],
    //                                                     scorer);
    //               });
    // auto merged_bigrams =
    //    v1::union_merge(std::move(essential_bigram_cursors),
    //                    initial_payload,
    //                    [&](auto& acc, auto& cursor, auto term_idx) {
    //                        auto payload = cursor.payload();
    //                        acc[essential_bigrams[term_idx].first] = std::get<0>(payload);
    //                        acc[essential_bigrams[term_idx].second] = std::get<1>(payload);
    //                        return acc;
    //                    });

    // auto accumulate = [&](auto& acc, auto& cursor, auto /* union_idx */) {
    //    auto payload = cursor.payload();
    //    for (auto idx = 0; idx < acc.size(); idx += 1) {
    //        acc[idx] = payload[idx];
    //    }
    //    return acc;
    //};
    // auto merged = v1::variadic_union_merge(
    //    initial_payload,
    //    std::make_tuple(std::move(merged_unigrams), std::move(merged_bigrams)),
    //    std::make_tuple(accumulate, accumulate));

    std::vector<decltype(index.max_scored_cursor(0, scorer))> lookup_cursors;
    for (auto idx = 0; idx < query.terms.size(); idx += 1) {
        if (not is_essential[idx]) {
            lookup_cursors.push_back(index.max_scored_cursor(query.terms[idx], scorer));
        }
    }
    // std::transform(query.terms.begin(),
    //               query.terms.end(),
    //               std::back_inserter(lookup_cursors),
    //               [&](auto term) { return index.scored_cursor(term, scorer); });

    // v1::for_each(merged, [&](auto& cursor) {
    v1::for_each(merged_unigrams, [&](auto& cursor) {
        auto docid = cursor.value();
        auto score = cursor.payload();
        auto score_bound = std::accumulate(
            lookup_cursors.begin(), lookup_cursors.end(), score, [](auto acc, auto&& cursor) {
                return acc + cursor.max_score();
            });
        if (not topk.would_enter(score_bound)) {
            return;
        }
        for (auto lookup_cursor : lookup_cursors) {
            // lookup_cursor.advance();
            lookup_cursor.advance_to_geq(docid);
            if (PISA_UNLIKELY(lookup_cursor.value() == docid)) {
                score += lookup_cursor.payload();
            }
            if (not topk.would_enter(score - lookup_cursor.max_score())) {
                return;
            }
        }
        topk.insert(score, docid);
        // auto docid = cursor.value();
        // auto partial_scores = cursor.payload();
        // float score = 0.0F;
        // for (auto idx = 0; idx < partial_scores.size(); idx += 1) {
        //    score += partial_scores[idx];
        //    // if (partial_scores[idx] > 0.0F) {
        //    //    score += partial_scores[idx];
        //    //}
        //    // else if (not is_essential[idx]) {
        //    //    lookup_cursors[idx].advance_to_geq(docid);
        //    //    if (lookup_cursors[idx].value() == docid) {
        //    //        score += lookup_cursors[idx].payload();
        //    //    }
        //    //}
        //}
        // topk.insert(score, docid);
    });
    return topk;
}

} // namespace pisa::v1
