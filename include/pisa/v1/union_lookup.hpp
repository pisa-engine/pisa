#pragma once

#include <algorithm>

#include <range/v3/view/reverse.hpp>

#include "v1/query.hpp"

namespace pisa::v1 {

template <typename Cursor>
void partition_by_essential(gsl::span<Cursor> cursors, gsl::span<std::size_t> essential_indices)
{
    if (essential_indices.empty()) {
        return;
    }
    std::sort(essential_indices.begin(), essential_indices.end());
    if (essential_indices[essential_indices.size() - 1] >= cursors.size()) {
        throw std::logic_error("Essential index too large");
    }
    auto left = 0;
    auto right = cursors.size() - 1;
    auto eidx = 0;
    while (left < right && eidx < essential_indices.size()) {
        if (left < essential_indices[eidx]) {
            left += 1;
        } else {
            std::swap(cursors[left], cursors[right]);
            right -= 1;
            eidx += 1;
        }
    }
}

template <typename Index, typename Scorer, typename Analyzer = void>
auto unigram_union_lookup(
    Query query, Index const& index, topk_queue topk, Scorer&& scorer, Analyzer* analyzer = nullptr)
{
    if (not query.threshold) {
        throw std::invalid_argument("Must provide threshold to the query");
    }
    if (not query.list_selection) {
        throw std::invalid_argument("Must provide essential list selection");
    }
    if (not query.list_selection->bigrams.empty()) {
        throw std::invalid_argument("This algorithm only supports unigrams");
    }

    topk.set_threshold(*query.threshold);

    using cursor_type = decltype(index.max_scored_cursor(0, scorer));
    using payload_type = decltype(std::declval<cursor_type>().payload());

    auto cursors = index.max_scored_cursors(gsl::make_span(query.terms), scorer);
    partition_by_essential(gsl::make_span(cursors), gsl::make_span(query.list_selection->unigrams));
    auto non_essential_count = cursors.size() - query.list_selection->unigrams.size();
    std::sort(cursors.begin(),
              std::next(cursors.begin(), non_essential_count),
              [](auto&& lhs, auto&& rhs) { return lhs.max_score() < rhs.max_score(); });

    std::vector<payload_type> upper_bounds(non_essential_count);
    upper_bounds[0] = cursors[0].max_score();
    for (size_t idx = 1; idx < non_essential_count; idx += 1) {
        upper_bounds[idx] = upper_bounds[idx - 1] + cursors[idx].max_score();
    }

    auto merged_essential = v1::union_merge(gsl::make_span(cursors).subspan(non_essential_count),
                                            0.0F,
                                            [&](auto acc, auto& cursor, auto /*term_idx*/) {
                                                if constexpr (not std::is_void_v<Analyzer>) {
                                                    analyzer->posting();
                                                }
                                                return acc + cursor.payload();
                                            });

    auto lookup_cursors = gsl::make_span(cursors).first(non_essential_count);
    v1::for_each(merged_essential, [&](auto& cursor) {
        if constexpr (not std::is_void_v<Analyzer>) {
            analyzer->document();
        }
        auto docid = cursor.value();
        auto score = cursor.payload();
        for (auto lookup_cursor_idx = non_essential_count - 1; lookup_cursor_idx + 1 > 0;
             lookup_cursor_idx -= 1) {
            if (not topk.would_enter(score + upper_bounds[lookup_cursor_idx])) {
                return;
            }
            cursors[lookup_cursor_idx].advance_to_geq(docid);
            if constexpr (not std::is_void_v<Analyzer>) {
                analyzer->lookup();
            }
            if (PISA_UNLIKELY(cursors[lookup_cursor_idx].value() == docid)) {
                score += cursors[lookup_cursor_idx].payload();
            }
        }
        if constexpr (not std::is_void_v<Analyzer>) {
            analyzer->insert();
        }
        topk.insert(score, docid);
    });
    return topk;
}

template <typename Index, typename Scorer, typename Analyzer = void>
auto maxscore_union_lookup(Query const& query,
                           Index const& index,
                           topk_queue topk,
                           Scorer&& scorer,
                           Analyzer* analyzer = nullptr)
{
    if (not query.threshold) {
        throw std::invalid_argument("Must provide threshold to the query");
    }

    topk.set_threshold(*query.threshold);

    using cursor_type = decltype(index.max_scored_cursor(0, scorer));
    using payload_type = decltype(std::declval<cursor_type>().payload());

    auto cursors = index.max_scored_cursors(gsl::make_span(query.terms), scorer);
    std::sort(cursors.begin(), cursors.end(), [](auto&& lhs, auto&& rhs) {
        return lhs.max_score() < rhs.max_score();
    });

    std::vector<payload_type> upper_bounds(cursors.size());
    upper_bounds[0] = cursors[0].max_score();
    for (size_t i = 1; i < cursors.size(); ++i) {
        upper_bounds[i] = upper_bounds[i - 1] + cursors[i].max_score();
    }
    std::size_t non_essential_count = 0;
    while (non_essential_count < cursors.size()
           && upper_bounds[non_essential_count] < *query.threshold) {
        non_essential_count += 1;
    }

    std::vector<std::size_t> unigrams(cursors.size() - non_essential_count);
    std::iota(unigrams.begin(), unigrams.end(), non_essential_count);
    Query query_with_selections = query;
    query_with_selections.list_selection =
        tl::make_optional(ListSelection{.unigrams = {}, .bigrams = {}});
    return unigram_union_lookup(std::move(query_with_selections),
                                index,
                                std::move(topk),
                                std::forward<Scorer>(scorer),
                                analyzer);

    // auto merged_essential = v1::union_merge(gsl::make_span(cursors).subspan(non_essential_count),
    //                                        0.0F,
    //                                        [&](auto acc, auto& cursor, auto /*term_idx*/) {
    //                                            if constexpr (not std::is_void_v<Analyzer>) {
    //                                                analyzer->posting();
    //                                            }
    //                                            return acc + cursor.payload();
    //                                        });

    // auto lookup_cursors = gsl::make_span(cursors).first(non_essential_count);
    // v1::for_each(merged_essential, [&](auto& cursor) {
    //    if constexpr (not std::is_void_v<Analyzer>) {
    //        analyzer->document();
    //    }
    //    auto docid = cursor.value();
    //    auto score = cursor.payload();
    //    for (auto lookup_cursor_idx = non_essential_count - 1; lookup_cursor_idx + 1 > 0;
    //         lookup_cursor_idx -= 1) {
    //        if (not topk.would_enter(score + upper_bounds[lookup_cursor_idx])) {
    //            return;
    //        }
    //        cursors[lookup_cursor_idx].advance_to_geq(docid);
    //        if constexpr (not std::is_void_v<Analyzer>) {
    //            analyzer->lookup();
    //        }
    //        if (PISA_UNLIKELY(cursors[lookup_cursor_idx].value() == docid)) {
    //            score += cursors[lookup_cursor_idx].payload();
    //        }
    //    }
    //    if constexpr (not std::is_void_v<Analyzer>) {
    //        analyzer->insert();
    //    }
    //    topk.insert(score, docid);
    //});
    // return topk;
}

template <typename Index, typename Scorer>
struct MaxscoreUnionLookupAnalyzer {
    MaxscoreUnionLookupAnalyzer(Index const& index, Scorer scorer)
        : m_index(index), m_scorer(std::move(scorer))
    {
        std::cout << fmt::format("documents\tpostings\tinserts\tlookups\n");
    }

    void reset_current()
    {
        m_current_documents = 0;
        m_current_postings = 0;
        m_current_lookups = 0;
        m_current_inserts = 0;
    }

    void operator()(Query const& query)
    {
        using cursor_type = decltype(m_index.max_scored_cursor(0, m_scorer));
        using value_type = decltype(m_index.max_scored_cursor(0, m_scorer).value());

        reset_current();
        topk_queue topk(query.k);
        maxscore_union_lookup(query, m_index, topk, m_scorer, this);
        std::cout << fmt::format("{}\t{}\t{}\t{}\n",
                                 m_current_documents,
                                 m_current_postings,
                                 m_current_inserts,
                                 m_current_lookups);
        m_documents += m_current_documents;
        m_postings += m_current_postings;
        m_lookups += m_current_lookups;
        m_inserts += m_current_inserts;
        m_count += 1;
    }

    void summarize() &&
    {
        std::cerr << fmt::format(
            "=== SUMMARY ===\nAverage:\n- documents:\t{}\n"
            "- postings:\t{}\n- inserts:\t{}\n- lookups:\t{}\n",
            static_cast<float>(m_documents) / m_count,
            static_cast<float>(m_postings) / m_count,
            static_cast<float>(m_inserts) / m_count,
            static_cast<float>(m_lookups) / m_count);
    }

    void document() { m_current_documents += 1; }
    void posting() { m_current_postings += 1; }
    void lookup() { m_current_lookups += 1; }
    void insert() { m_current_inserts += 1; }

   private:
    std::size_t m_current_documents = 0;
    std::size_t m_current_postings = 0;
    std::size_t m_current_lookups = 0;
    std::size_t m_current_inserts = 0;

    std::size_t m_documents = 0;
    std::size_t m_postings = 0;
    std::size_t m_lookups = 0;
    std::size_t m_inserts = 0;
    std::size_t m_count = 0;
    Index const& m_index;
    Scorer m_scorer;
};

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

    topk.set_threshold(*query.threshold);

    std::vector<bool> is_essential(query.terms.size(), false);
    for (auto idx : essential_unigrams) {
        is_essential[idx] = true;
    }

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

    std::vector<decltype(index.max_scored_cursor(0, scorer))> lookup_cursors;
    for (auto idx = 0; idx < query.terms.size(); idx += 1) {
        if (not is_essential[idx]) {
            lookup_cursors.push_back(index.max_scored_cursor(query.terms[idx], scorer));
        }
    }
    std::sort(lookup_cursors.begin(), lookup_cursors.end(), [](auto&& lhs, auto&& rhs) {
        return lhs.max_score() > rhs.max_score();
    });
    auto lookup_cursors_upper_bound = std::accumulate(
        lookup_cursors.begin(), lookup_cursors.end(), 0.0F, [](auto acc, auto&& cursor) {
            return acc + cursor.max_score();
        });

    v1::for_each(merged, [&](auto& cursor) {
        auto docid = cursor.value();
        auto scores = cursor.payload();
        auto score = std::accumulate(scores.begin(), scores.end(), 0.0F, std::plus{});
        auto upper_bound = score + lookup_cursors_upper_bound;
        for (auto lookup_cursor : lookup_cursors) {
            if (not topk.would_enter(upper_bound)) {
                return;
            }
            lookup_cursor.advance_to_geq(docid);
            if (PISA_UNLIKELY(lookup_cursor.value() == docid)) {
                auto partial_score = lookup_cursor.payload();
                score += partial_score;
                upper_bound += partial_score;
            }
            upper_bound -= lookup_cursor.max_score();
        }
        topk.insert(score, docid);
    });
    return topk;
}

} // namespace pisa::v1
