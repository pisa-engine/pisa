#pragma once

#include <algorithm>

#include <range/v3/range/conversion.hpp>
#include <range/v3/view/filter.hpp>
#include <range/v3/view/reverse.hpp>
#include <range/v3/view/set_algorithm.hpp>

#include "v1/algorithm.hpp"
#include "v1/query.hpp"

namespace pisa::v1 {

namespace detail {
    template <typename Cursors, typename UpperBounds, typename Analyzer = void>
    auto unigram_union_lookup(Cursors cursors,
                              UpperBounds upper_bounds,
                              std::size_t non_essential_count,
                              topk_queue topk,
                              Analyzer* analyzer = nullptr)
    {
        using payload_type = decltype(std::declval<typename Cursors::value_type>().payload());

        auto merged_essential =
            v1::union_merge(gsl::make_span(cursors).subspan(non_essential_count),
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
} // namespace detail

template <typename Index, typename Scorer, typename Analyzer = void>
auto unigram_union_lookup(Query const& query,
                          Index const& index,
                          topk_queue topk,
                          Scorer&& scorer,
                          Analyzer* analyzer = nullptr)
{
    auto const& term_ids = query.get_term_ids();
    if (term_ids.empty()) {
        return topk;
    }
    if (not query.threshold()) {
        throw std::invalid_argument("Must provide threshold to the query");
    }
    if (not query.selections()) {
        throw std::invalid_argument("Must provide essential list selection");
    }
    if (not query.selections()->bigrams.empty()) {
        throw std::invalid_argument("This algorithm only supports unigrams");
    }
    auto const& selections = query.get_selections();

    topk.set_threshold(*query.threshold());

    using cursor_type = decltype(index.max_scored_cursor(0, scorer));
    using payload_type = decltype(std::declval<cursor_type>().payload());

    auto non_essential_terms =
        ranges::views::set_difference(term_ids, selections.unigrams) | ranges::to_vector;

    std::vector<cursor_type> cursors;
    for (auto non_essential_term : non_essential_terms) {
        cursors.push_back(index.max_scored_cursor(non_essential_term, scorer));
    }
    auto non_essential_count = cursors.size();
    std::sort(cursors.begin(), cursors.end(), [](auto&& lhs, auto&& rhs) {
        return lhs.max_score() < rhs.max_score();
    });
    for (auto essential_term : selections.unigrams) {
        cursors.push_back(index.max_scored_cursor(essential_term, scorer));
    }

    std::vector<payload_type> upper_bounds(cursors.size());
    upper_bounds[0] = cursors[0].max_score();
    for (size_t i = 1; i < cursors.size(); ++i) {
        upper_bounds[i] = upper_bounds[i - 1] + cursors[i].max_score();
    }
    return detail::unigram_union_lookup(std::move(cursors),
                                        std::move(upper_bounds),
                                        non_essential_count,
                                        std::move(topk),
                                        analyzer);
}

template <typename Index, typename Scorer, typename Analyzer = void>
auto maxscore_union_lookup(Query const& query,
                           Index const& index,
                           topk_queue topk,
                           Scorer&& scorer,
                           Analyzer* analyzer = nullptr)
{
    auto const& term_ids = query.get_term_ids();
    if (term_ids.empty()) {
        return topk;
    }
    auto threshold = query.get_threshold();
    topk.set_threshold(threshold);

    using cursor_type = decltype(index.max_scored_cursor(0, scorer));
    using payload_type = decltype(std::declval<cursor_type>().payload());

    auto cursors = index.max_scored_cursors(gsl::make_span(term_ids), scorer);
    std::sort(cursors.begin(), cursors.end(), [](auto&& lhs, auto&& rhs) {
        return lhs.max_score() < rhs.max_score();
    });

    std::vector<payload_type> upper_bounds(cursors.size());
    upper_bounds[0] = cursors[0].max_score();
    for (size_t i = 1; i < cursors.size(); ++i) {
        upper_bounds[i] = upper_bounds[i - 1] + cursors[i].max_score();
    }
    std::size_t non_essential_count = 0;
    while (non_essential_count < cursors.size() && upper_bounds[non_essential_count] < threshold) {
        non_essential_count += 1;
    }
    return detail::unigram_union_lookup(std::move(cursors),
                                        std::move(upper_bounds),
                                        non_essential_count,
                                        std::move(topk),
                                        analyzer);
}

template <typename Index, typename Scorer>
struct BaseUnionLookupAnalyzer {
    BaseUnionLookupAnalyzer(Index const& index, Scorer scorer)
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

    virtual void run(Query const& query, Index const& index, Scorer& scorer, topk_queue topk) = 0;

    void operator()(Query const& query)
    {
        auto const& term_ids = query.get_term_ids();
        if (term_ids.empty()) {
            return;
        }
        using cursor_type = decltype(m_index.max_scored_cursor(0, m_scorer));
        using value_type = decltype(m_index.max_scored_cursor(0, m_scorer).value());

        reset_current();
        run(query, m_index, m_scorer, topk_queue(query.k()));
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

template <typename Index, typename Scorer>
struct MaxscoreUnionLookupAnalyzer : public BaseUnionLookupAnalyzer<Index, Scorer> {
    MaxscoreUnionLookupAnalyzer(Index const& index, Scorer scorer)
        : BaseUnionLookupAnalyzer<Index, Scorer>(index, std::move(scorer))
    {
    }
    void run(Query const& query, Index const& index, Scorer& scorer, topk_queue topk) override
    {
        maxscore_union_lookup(query, index, std::move(topk), scorer, this);
    }
};

template <typename Index, typename Scorer>
struct UnigramUnionLookupAnalyzer : public BaseUnionLookupAnalyzer<Index, Scorer> {
    UnigramUnionLookupAnalyzer(Index const& index, Scorer scorer)
        : BaseUnionLookupAnalyzer<Index, Scorer>(index, std::move(scorer))
    {
    }
    void run(Query const& query, Index const& index, Scorer& scorer, topk_queue topk) override
    {
        unigram_union_lookup(query, index, std::move(topk), scorer, this);
    }
};

template <typename Index, typename Scorer>
struct UnionLookupAnalyzer : public BaseUnionLookupAnalyzer<Index, Scorer> {
    UnionLookupAnalyzer(Index const& index, Scorer scorer)
        : BaseUnionLookupAnalyzer<Index, Scorer>(index, std::move(scorer))
    {
    }
    void run(Query const& query, Index const& index, Scorer& scorer, topk_queue topk) override
    {
        if (query.get_term_ids().size() > 8) {
            maxscore_union_lookup(query, index, std::move(topk), scorer, this);
        } else {
            union_lookup(query, index, std::move(topk), scorer, this);
        }
    }
};

template <typename Index, typename Scorer>
struct TwoPhaseUnionLookupAnalyzer : public BaseUnionLookupAnalyzer<Index, Scorer> {
    TwoPhaseUnionLookupAnalyzer(Index const& index, Scorer scorer)
        : BaseUnionLookupAnalyzer<Index, Scorer>(index, std::move(scorer))
    {
    }
    void run(Query const& query, Index const& index, Scorer& scorer, topk_queue topk) override
    {
        if (query.get_term_ids().size() > 8) {
            maxscore_union_lookup(query, index, std::move(topk), scorer, this);
        } else {
            two_phase_union_lookup(query, index, std::move(topk), scorer, this);
        }
    }
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
template <typename Index, typename Scorer, typename Analyzer = void>
auto union_lookup(Query const& query,
                  Index const& index,
                  topk_queue topk,
                  Scorer&& scorer,
                  Analyzer* analyzer = nullptr)
{
    auto const& term_ids = query.get_term_ids();
    if (term_ids.empty()) {
        return topk;
    }
    if (term_ids.size() > 8) {
        throw std::invalid_argument(
            "Generic version of union-Lookup supported only for queries of length <= 8");
    }

    auto threshold = query.get_threshold();
    auto const& selections = query.get_selections();

    using bigram_cursor_type = std::decay_t<decltype(*index.scored_bigram_cursor(0, 0, scorer))>;

    auto& essential_unigrams = selections.unigrams;
    auto& essential_bigrams = selections.bigrams;

    auto non_essential_terms =
        ranges::views::set_difference(term_ids, essential_unigrams) | ranges::to_vector;

    topk.set_threshold(threshold);

    std::array<float, 8> initial_payload{
        0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0}; //, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0};

    std::vector<decltype(index.scored_cursor(0, scorer))> essential_unigram_cursors;
    std::transform(essential_unigrams.begin(),
                   essential_unigrams.end(),
                   std::back_inserter(essential_unigram_cursors),
                   [&](auto term) { return index.scored_cursor(term, scorer); });

    auto merged_unigrams = v1::union_merge(
        essential_unigram_cursors, initial_payload, [&](auto& acc, auto& cursor, auto term_idx) {
            if constexpr (not std::is_void_v<Analyzer>) {
                analyzer->posting();
            }
            acc[query.sorted_position(essential_unigrams[term_idx])] = cursor.payload();
            return acc;
        });

    std::vector<bigram_cursor_type> essential_bigram_cursors;
    for (auto [left, right] : essential_bigrams) {
        auto cursor = index.scored_bigram_cursor(left, right, scorer);
        if (not cursor) {
            throw std::runtime_error(fmt::format("Bigram not found: <{}, {}>", left, right));
        }
        essential_bigram_cursors.push_back(cursor.take().value());
    }

    auto merged_bigrams = v1::union_merge(
        std::move(essential_bigram_cursors),
        initial_payload,
        [&](auto& acc, auto& cursor, auto bigram_idx) {
            if constexpr (not std::is_void_v<Analyzer>) {
                analyzer->posting();
            }
            auto payload = cursor.payload();
            acc[query.sorted_position(essential_bigrams[bigram_idx].first)] = std::get<0>(payload);
            acc[query.sorted_position(essential_bigrams[bigram_idx].second)] = std::get<1>(payload);
            return acc;
        });

    auto accumulate = [&](auto& acc, auto& cursor, auto /* union_idx */) {
        auto payload = cursor.payload();
        for (auto idx = 0; idx < acc.size(); idx += 1) {
            if (acc[idx] == 0.0F) {
                acc[idx] = payload[idx];
            }
        }
        return acc;
    };
    auto merged = v1::variadic_union_merge(
        initial_payload,
        std::make_tuple(std::move(merged_unigrams), std::move(merged_bigrams)),
        std::make_tuple(accumulate, accumulate));

    auto lookup_cursors = [&]() {
        std::vector<std::pair<std::size_t, decltype(index.max_scored_cursor(0, scorer))>>
            lookup_cursors;
        auto pos = term_ids.begin();
        for (auto non_essential_term : non_essential_terms) {
            pos = std::find(pos, term_ids.end(), non_essential_term);
            assert(pos != term_ids.end());
            auto idx = std::distance(term_ids.begin(), pos);
            lookup_cursors.emplace_back(idx, index.max_scored_cursor(non_essential_term, scorer));
        }
        return lookup_cursors;
    }();
    std::sort(lookup_cursors.begin(), lookup_cursors.end(), [](auto&& lhs, auto&& rhs) {
        return lhs.second.max_score() > rhs.second.max_score();
    });
    auto lookup_cursors_upper_bound = std::accumulate(
        lookup_cursors.begin(), lookup_cursors.end(), 0.0F, [](auto acc, auto&& cursor) {
            return acc + cursor.second.max_score();
        });

    v1::for_each(merged, [&](auto& cursor) {
        if constexpr (not std::is_void_v<Analyzer>) {
            analyzer->document();
        }
        auto docid = cursor.value();
        auto scores = cursor.payload();
        auto score = std::accumulate(scores.begin(), scores.end(), 0.0F, std::plus{});
        auto upper_bound = score + lookup_cursors_upper_bound;
        for (auto& [idx, lookup_cursor] : lookup_cursors) {
            if (not topk.would_enter(upper_bound)) {
                return;
            }
            if (scores[idx] == 0) {
                lookup_cursor.advance_to_geq(docid);
                if constexpr (not std::is_void_v<Analyzer>) {
                    analyzer->lookup();
                }
                if (PISA_UNLIKELY(lookup_cursor.value() == docid)) {
                    auto partial_score = lookup_cursor.payload();
                    score += partial_score;
                    upper_bound += partial_score;
                }
            }
            upper_bound -= lookup_cursor.max_score();
        }
        topk.insert(score, docid);
        if constexpr (not std::is_void_v<Analyzer>) {
            analyzer->insert();
        }
    });
    return topk;
}

template <typename Index, typename Scorer, typename Analyzer = void>
auto two_phase_union_lookup(Query const& query,
                            Index const& index,
                            topk_queue topk,
                            Scorer&& scorer,
                            Analyzer* analyzer = nullptr)
{
    auto const& term_ids = query.get_term_ids();
    if (term_ids.empty()) {
        return topk;
    }
    if (term_ids.size() > 8) {
        throw std::invalid_argument(
            "Generic version of union-Lookup supported only for queries of length <= 8");
    }

    auto threshold = query.get_threshold();
    auto const& selections = query.get_selections();

    using bigram_cursor_type = std::decay_t<decltype(*index.scored_bigram_cursor(0, 0, scorer))>;

    auto& essential_unigrams = selections.unigrams;
    auto& essential_bigrams = selections.bigrams;

    auto non_essential_terms =
        ranges::views::set_difference(term_ids, essential_unigrams) | ranges::to_vector;

    topk.set_threshold(threshold);

    std::array<float, 8> initial_payload{
        0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0}; //, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0};

    std::vector<decltype(index.scored_cursor(0, scorer))> essential_unigram_cursors;
    std::transform(essential_unigrams.begin(),
                   essential_unigrams.end(),
                   std::back_inserter(essential_unigram_cursors),
                   [&](auto term) { return index.scored_cursor(term, scorer); });

    auto merged_unigrams = v1::union_merge(
        essential_unigram_cursors, initial_payload, [&](auto& acc, auto& cursor, auto term_idx) {
            if constexpr (not std::is_void_v<Analyzer>) {
                analyzer->posting();
            }
            acc[query.sorted_position(essential_unigrams[term_idx])] = cursor.payload();
            return acc;
        });

    std::vector<bigram_cursor_type> essential_bigram_cursors;
    for (auto [left, right] : essential_bigrams) {
        auto cursor = index.scored_bigram_cursor(left, right, scorer);
        if (not cursor) {
            throw std::runtime_error(fmt::format("Bigram not found: <{}, {}>", left, right));
        }
        essential_bigram_cursors.push_back(cursor.take().value());
    }

    auto merged_bigrams = v1::union_merge(
        std::move(essential_bigram_cursors),
        initial_payload,
        [&](auto& acc, auto& cursor, auto bigram_idx) {
            if constexpr (not std::is_void_v<Analyzer>) {
                analyzer->posting();
            }
            auto payload = cursor.payload();
            acc[query.sorted_position(essential_bigrams[bigram_idx].first)] = std::get<0>(payload);
            acc[query.sorted_position(essential_bigrams[bigram_idx].second)] = std::get<1>(payload);
            return acc;
        });

    auto lookup_cursors = [&]() {
        std::vector<std::pair<std::size_t, decltype(index.max_scored_cursor(0, scorer))>>
            lookup_cursors;
        auto pos = term_ids.begin();
        for (auto non_essential_term : non_essential_terms) {
            pos = std::find(pos, term_ids.end(), non_essential_term);
            assert(pos != term_ids.end());
            auto idx = std::distance(term_ids.begin(), pos);
            lookup_cursors.emplace_back(idx, index.max_scored_cursor(non_essential_term, scorer));
        }
        return lookup_cursors;
    }();
    std::sort(lookup_cursors.begin(), lookup_cursors.end(), [](auto&& lhs, auto&& rhs) {
        return lhs.second.max_score() > rhs.second.max_score();
    });
    auto lookup_cursors_upper_bound = std::accumulate(
        lookup_cursors.begin(), lookup_cursors.end(), 0.0F, [](auto acc, auto&& cursor) {
            return acc + cursor.second.max_score();
        });

    v1::for_each(merged_unigrams, [&](auto& cursor) {
        if constexpr (not std::is_void_v<Analyzer>) {
            analyzer->document();
        }
        auto docid = cursor.value();
        auto scores = cursor.payload();
        auto score = std::accumulate(scores.begin(), scores.end(), 0.0F, std::plus{});
        auto upper_bound = score + lookup_cursors_upper_bound;
        for (auto& [idx, lookup_cursor] : lookup_cursors) {
            if (not topk.would_enter(upper_bound)) {
                return;
            }
            if (scores[idx] == 0) {
                lookup_cursor.advance_to_geq(docid);
                if constexpr (not std::is_void_v<Analyzer>) {
                    analyzer->lookup();
                }
                if (PISA_UNLIKELY(lookup_cursor.value() == docid)) {
                    auto partial_score = lookup_cursor.payload();
                    score += partial_score;
                    upper_bound += partial_score;
                }
            }
            upper_bound -= lookup_cursor.max_score();
        }
        topk.insert(score, docid);
        if constexpr (not std::is_void_v<Analyzer>) {
            analyzer->insert();
        }
    });

    v1::for_each(merged_bigrams, [&](auto& cursor) {
        if constexpr (not std::is_void_v<Analyzer>) {
            analyzer->document();
        }
        auto docid = cursor.value();
        auto scores = cursor.payload();
        auto score = std::accumulate(scores.begin(), scores.end(), 0.0F, std::plus{});
        auto upper_bound = score + lookup_cursors_upper_bound;
        for (auto& [idx, lookup_cursor] : lookup_cursors) {
            if (not topk.would_enter(upper_bound)) {
                return;
            }
            if (scores[idx] == 0) {
                lookup_cursor.advance_to_geq(docid);
                if constexpr (not std::is_void_v<Analyzer>) {
                    analyzer->lookup();
                }
                if (PISA_UNLIKELY(lookup_cursor.value() == docid)) {
                    auto partial_score = lookup_cursor.payload();
                    score += partial_score;
                    upper_bound += partial_score;
                }
            }
            upper_bound -= lookup_cursor.max_score();
        }
        topk.insert(score, docid);
        if constexpr (not std::is_void_v<Analyzer>) {
            analyzer->insert();
        }
    });
    return topk;
}

} // namespace pisa::v1
