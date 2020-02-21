#pragma once

#include <algorithm>

#include <range/v3/range/conversion.hpp>
#include <range/v3/view/filter.hpp>
#include <range/v3/view/set_algorithm.hpp>
#include <range/v3/view/transform.hpp>

#include "v1/algorithm.hpp"
#include "v1/cursor/labeled_cursor.hpp"
#include "v1/cursor/lookup_transform.hpp"
#include "v1/cursor/reference.hpp"
#include "v1/cursor/transform.hpp"
#include "v1/cursor_accumulator.hpp"
#include "v1/maxscore_union_lookup.hpp"
#include "v1/query.hpp"
#include "v1/runtime_assert.hpp"
#include "v1/unigram_union_lookup.hpp"
#include "v1/union_lookup_join.hpp"

namespace pisa::v1 {

template <typename Index, typename Scorer, typename LookupCursors>
auto filter_bigram_lookup_cursors(
    Index const& index, Scorer&& scorer, LookupCursors&& lookup_cursors, TermId left, TermId right)
{
    return ranges::views::filter(
               lookup_cursors,
               [&](auto&& cursor) { return cursor.label() != left && cursor.label() != right; })
           //| ranges::views::transform([](auto&& cursor) { return ref(cursor); })
           | ranges::views::transform(
               [&](auto&& cursor) { return index.max_scored_cursor(cursor.label(), scorer); })
           | ranges::to_vector;
}

/// This algorithm...
template <typename Index,
          typename Scorer,
          typename InspectUnigram = void,
          typename InspectBigram = void>
auto lookup_union(Query const& query,
                  Index const& index,
                  topk_queue topk,
                  Scorer&& scorer,
                  InspectUnigram* inspect_unigram = nullptr,
                  InspectBigram* inspect_bigram = nullptr)
{
    using bigram_cursor_type = std::decay_t<decltype(*index.scored_bigram_cursor(0, 0, scorer))>;

    auto const& term_ids = query.get_term_ids();
    if (term_ids.empty()) {
        return topk;
    }

    auto threshold = query.get_threshold();
    topk.set_threshold(threshold);
    auto is_above_threshold = [&](auto score) { return topk.would_enter(score); };

    auto const& selections = query.get_selections();
    auto& essential_unigrams = selections.unigrams;
    auto& essential_bigrams = selections.bigrams;

    if constexpr (not std::is_void_v<InspectUnigram>) {
        inspect_unigram->essential(essential_unigrams.size());
    }
    if constexpr (not std::is_void_v<InspectBigram>) {
        inspect_bigram->essential(essential_bigrams.size());
    }

    auto non_essential_terms =
        ranges::views::set_difference(term_ids, essential_unigrams) | ranges::to_vector;

    auto lookup_cursors = index.cursors(non_essential_terms, [&](auto&& index, auto term) {
        return label(index.max_scored_cursor(term, scorer), term);
    });
    ranges::sort(lookup_cursors, std::greater{}, func::max_score{});
    auto unigram_cursor =
        join_union_lookup(index.max_scored_cursors(gsl::make_span(essential_unigrams), scorer),
                          gsl::make_span(lookup_cursors),
                          0.0F,
                          accumulators::Add{},
                          is_above_threshold,
                          inspect_unigram);

    using lookup_transform_type =
        LookupTransform<typename decltype(filter_bigram_lookup_cursors(
                            index, scorer, lookup_cursors, 0, 1))::value_type,
                        decltype(is_above_threshold),
                        InspectBigram>;
    using transform_payload_cursor_type =
        TransformPayloadCursor<bigram_cursor_type, lookup_transform_type>;

    std::vector<transform_payload_cursor_type> bigram_cursors;

    for (auto [left, right] : essential_bigrams) {
        auto cursor = index.scored_bigram_cursor(left, right, scorer);
        if (not cursor) {
            throw std::runtime_error(fmt::format("Bigram not found: <{}, {}>", left, right));
        }
        auto bigram_lookup_cursors =
            filter_bigram_lookup_cursors(index, scorer, lookup_cursors, left, right);
        auto lookup_cursors_upper_bound =
            std::accumulate(bigram_lookup_cursors.begin(),
                            bigram_lookup_cursors.end(),
                            0.0F,
                            [](auto acc, auto&& cursor) { return acc + cursor.max_score(); });
        bigram_cursors.emplace_back(std::move(*cursor.take()),
                                    LookupTransform(std::move(bigram_lookup_cursors),
                                                    lookup_cursors_upper_bound,
                                                    is_above_threshold,
                                                    inspect_bigram));
    }

    auto accumulate = [&](float acc, auto& cursor, [[maybe_unused]] auto idx) {
        return acc == 0 ? cursor.payload() : acc;
    };
    auto bigram_cursor = union_merge(
        std::move(bigram_cursors), 0.0F, [&](float acc, auto& cursor, [[maybe_unused]] auto idx) {
            if constexpr (not std::is_void_v<InspectUnigram>) {
                inspect_bigram->posting();
            }
            return acc == 0 ? cursor.payload() : acc;
        });
    auto merged = v1::variadic_union_merge(
        0.0F,
        std::make_tuple(std::move(unigram_cursor), std::move(bigram_cursor)),
        std::make_tuple(accumulate, accumulate));

    v1::for_each(merged, [&](auto&& cursor) {
        if constexpr (not std::is_void_v<InspectUnigram>) {
            if (topk.insert(cursor.payload(), cursor.value())) {
                inspect_unigram->insert();
            }
        } else {
            topk.insert(cursor.payload(), cursor.value());
        }
    });
    return topk;
}

template <typename Cursor, typename InspectInserts = void, typename InspectPostings = void>
auto accumulate_cursor_to_heap(Cursor&& cursor,
                               std::size_t k,
                               float threshold = 0.0,
                               InspectInserts* inspect_inserts = nullptr,
                               InspectPostings* inspect_postings = nullptr)
{
    topk_queue heap(k);
    heap.set_threshold(threshold);
    v1::for_each(cursor, [&](auto&& cursor) {
        if constexpr (not std::is_void_v<InspectPostings>) {
            inspect_postings->posting();
        }
        if constexpr (not std::is_void_v<InspectInserts>) {
            if (heap.insert(cursor.payload(), cursor.value())) {
                inspect_inserts->insert();
            }
        } else {
            heap.insert(cursor.payload(), cursor.value());
        }
    });
    return heap;
}

/// This algorithm...
template <typename Index,
          typename Scorer,
          typename InspectUnigram = void,
          typename InspectBigram = void>
auto lookup_union_eaat(Query const& query,
                       Index const& index,
                       topk_queue topk,
                       Scorer&& scorer,
                       InspectUnigram* inspect_unigram = nullptr,
                       InspectBigram* inspect_bigram = nullptr)
{
    using bigram_cursor_type = std::decay_t<decltype(*index.scored_bigram_cursor(0, 0, scorer))>;
    using lookup_cursor_type = std::decay_t<decltype(index.max_scored_cursor(0, scorer))>;

    auto const& term_ids = query.get_term_ids();
    if (term_ids.empty()) {
        return topk;
    }

    auto threshold = query.get_threshold();
    topk.set_threshold(threshold);
    auto is_above_threshold = [&](auto score) { return topk.would_enter(score); };

    auto const& selections = query.get_selections();
    auto& essential_unigrams = selections.unigrams;
    auto& essential_bigrams = selections.bigrams;

    if constexpr (not std::is_void_v<InspectUnigram>) {
        inspect_unigram->essential(essential_unigrams.size());
    }
    if constexpr (not std::is_void_v<InspectBigram>) {
        inspect_bigram->essential(essential_bigrams.size());
    }

    auto non_essential_terms =
        ranges::views::set_difference(term_ids, essential_unigrams) | ranges::to_vector;

    auto unigram_cursor = [&]() {
        auto lookup_cursors = index.max_scored_cursors(gsl::make_span(non_essential_terms), scorer);
        ranges::sort(lookup_cursors,
                     [](auto&& lhs, auto&& rhs) { return lhs.max_score() > rhs.max_score(); });
        auto essential_cursors =
            index.max_scored_cursors(gsl::make_span(essential_unigrams), scorer);

        return join_union_lookup(std::move(essential_cursors),
                                 std::move(lookup_cursors),
                                 0.0F,
                                 accumulators::Add{},
                                 is_above_threshold,
                                 inspect_unigram);
    }();

    auto unigram_heap =
        accumulate_cursor_to_heap(unigram_cursor, topk.size(), threshold, inspect_unigram);

    using lookup_transform_type =
        LookupTransform<lookup_cursor_type, decltype(is_above_threshold), InspectBigram>;
    using transform_payload_cursor_type =
        TransformPayloadCursor<bigram_cursor_type, lookup_transform_type>;

    std::vector<typename topk_queue::entry_type> entries(unigram_heap.topk().begin(),
                                                         unigram_heap.topk().end());

    for (auto [left, right] : essential_bigrams) {
        auto cursor = index.scored_bigram_cursor(left, right, scorer);
        if (not cursor) {
            throw std::runtime_error(fmt::format("Bigram not found: <{}, {}>", left, right));
        }
        std::vector<TermId> essential_terms{left, right};
        auto lookup_terms =
            ranges::views::set_difference(non_essential_terms, essential_terms) | ranges::to_vector;

        auto lookup_cursors = index.max_scored_cursors(lookup_terms, scorer);
        ranges::sort(lookup_cursors,
                     [](auto&& lhs, auto&& rhs) { return lhs.max_score() > rhs.max_score(); });

        auto lookup_cursors_upper_bound = std::accumulate(
            lookup_cursors.begin(), lookup_cursors.end(), 0.0F, [](auto acc, auto&& cursor) {
                return acc + cursor.max_score();
            });

        auto heap = accumulate_cursor_to_heap(
            transform_payload_cursor_type(std::move(*cursor.take()),
                                          lookup_transform_type(std::move(lookup_cursors),
                                                                lookup_cursors_upper_bound,
                                                                is_above_threshold,
                                                                inspect_bigram)),
            topk.size(),
            threshold,
            inspect_bigram,
            inspect_bigram);
        std::copy(heap.topk().begin(), heap.topk().end(), std::back_inserter(entries));
    }
    std::sort(entries.begin(), entries.end(), [](auto&& lhs, auto&& rhs) {
        if (lhs.second == rhs.second) {
            return lhs.first > rhs.first;
        }
        return lhs.second < rhs.second;
    });
    auto end = std::unique(entries.begin(), entries.end(), [](auto&& lhs, auto&& rhs) {
        return lhs.second == rhs.second;
    });
    entries.erase(end, entries.end());
    std::sort(entries.begin(), entries.end(), [](auto&& lhs, auto&& rhs) {
        return lhs.first > rhs.first;
    });
    if (entries.size() > topk.size()) {
        entries.erase(std::next(entries.begin(), topk.size()), entries.end());
    }

    for (auto entry : entries) {
        topk.insert(entry.first, entry.second);
    }

    return topk;
}

template <typename Index, typename Scorer, typename Inspect = void>
auto union_lookup(Query const& query,
                  Index const& index,
                  topk_queue topk,
                  Scorer&& scorer,
                  Inspect* inspect = nullptr)
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

    std::array<float, 8> initial_payload{0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0};

    if constexpr (not std::is_void_v<Inspect>) {
        inspect->essential(essential_unigrams.size() + essential_bigrams.size());
    }

    std::vector<decltype(index.scored_cursor(0, scorer))> essential_unigram_cursors;
    std::transform(essential_unigrams.begin(),
                   essential_unigrams.end(),
                   std::back_inserter(essential_unigram_cursors),
                   [&](auto term) { return index.scored_cursor(term, scorer); });

    std::vector<std::size_t> unigram_query_positions(essential_unigrams.size());
    for (std::size_t unigram_position = 0; unigram_position < essential_unigrams.size();
         unigram_position += 1) {
        unigram_query_positions[unigram_position] =
            query.sorted_position(essential_unigrams[unigram_position]);
    }
    auto merged_unigrams = v1::union_merge(
        essential_unigram_cursors, initial_payload, [&](auto& acc, auto& cursor, auto term_idx) {
            if constexpr (not std::is_void_v<Inspect>) {
                inspect->posting();
            }
            acc[unigram_query_positions[term_idx]] = cursor.payload();
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

    std::vector<std::pair<std::size_t, std::size_t>> bigram_query_positions(
        essential_bigrams.size());
    for (std::size_t bigram_position = 0; bigram_position < essential_bigrams.size();
         bigram_position += 1) {
        bigram_query_positions[bigram_position] =
            std::make_pair(query.sorted_position(essential_bigrams[bigram_position].first),
                           query.sorted_position(essential_bigrams[bigram_position].second));
    }
    auto merged_bigrams = v1::union_merge(std::move(essential_bigram_cursors),
                                          initial_payload,
                                          [&](auto& acc, auto& cursor, auto bigram_idx) {
                                              if constexpr (not std::is_void_v<Inspect>) {
                                                  inspect->posting();
                                              }
                                              auto payload = cursor.payload();
                                              auto query_positions =
                                                  bigram_query_positions[bigram_idx];
                                              acc[query_positions.first] = std::get<0>(payload);
                                              acc[query_positions.second] = std::get<1>(payload);
                                              return acc;
                                          });

    auto accumulate = [&](auto& acc, auto& cursor, auto /* union_idx */) {
        auto payload = cursor.payload();
        for (auto idx = 0; idx < acc.size(); idx += 1) {
            if (acc[idx] == 0) {
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
        if constexpr (not std::is_void_v<Inspect>) {
            inspect->document();
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
                if constexpr (not std::is_void_v<Inspect>) {
                    inspect->lookup();
                }
                if (PISA_UNLIKELY(lookup_cursor.value() == docid)) {
                    auto partial_score = lookup_cursor.payload();
                    score += partial_score;
                    upper_bound += partial_score;
                }
            }
            upper_bound -= lookup_cursor.max_score();
        }
        if constexpr (not std::is_void_v<Inspect>) {
            if (topk.insert(score, docid)) {
                inspect->insert();
            }
        } else {
            topk.insert(score, docid);
        }
    });
    return topk;
}

inline auto precompute_next_lookup(std::size_t essential_count,
                                   std::size_t non_essential_count,
                                   std::vector<std::vector<std::uint32_t>> const& essential_bigrams)
{
    runtime_assert(essential_count + non_essential_count <= 8).or_throw("Must be shorter than 9");
    std::uint32_t term_count = essential_count + non_essential_count;
    std::vector<std::int32_t> next_lookup((term_count + 1) * (1U << term_count), -1);
    auto unnecessary = [&](auto p, auto state) {
        if (((1U << p) & state) > 0) {
            return true;
        }
        for (auto k : essential_bigrams[p]) {
            if (((1U << k) & state) > 0) {
                return true;
            }
        }
        return false;
    };
    for (auto term_idx = essential_count; term_idx < term_count; term_idx += 1) {
        for (std::uint32_t state = 0; state < (1U << term_count); state += 1) {
            auto p = term_idx;
            while (p < term_count && unnecessary(p, state)) {
                ++p;
            }
            if (p == term_count) {
                next_lookup[(term_idx << term_count) + state] = -1;
            } else {
                next_lookup[(term_idx << term_count) + state] = p;
            }
        }
    }
    return next_lookup;
}

template <typename Index, typename Scorer, typename Inspect = void>
auto union_lookup_plus(Query const& query,
                       Index const& index,
                       topk_queue topk,
                       Scorer&& scorer,
                       Inspect* inspect = nullptr)
{
    using bigram_cursor_type =
        LabeledCursor<std::decay_t<decltype(*index.scored_bigram_cursor(0, 0, scorer))>,
                      std::pair<std::uint32_t, std::uint32_t>>;

    auto term_ids = gsl::make_span(query.get_term_ids());
    std::size_t term_count = term_ids.size();
    if (term_ids.empty()) {
        return topk;
    }
    runtime_assert(term_ids.size() <= 8)
        .or_throw("Generic version of union-Lookup supported only for queries of length <= 8");
    topk.set_threshold(query.get_threshold());
    auto const& selections = query.get_selections();
    auto& essential_unigrams = selections.unigrams;
    auto& essential_bigrams = selections.bigrams;

    auto non_essential_terms =
        ranges::views::set_difference(term_ids, essential_unigrams) | ranges::to_vector;

    std::array<float, 8> initial_payload{0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0};

    if constexpr (not std::is_void_v<Inspect>) {
        inspect->essential(essential_unigrams.size() + essential_bigrams.size());
    }

    auto essential_unigram_cursors =
        index.cursors(essential_unigrams, [&](auto&& index, auto term) {
            return label(index.scored_cursor(term, scorer), term);
        });

    auto lookup_cursors =
        index.cursors(gsl::make_span(non_essential_terms), [&](auto&& index, auto term) {
            return label(index.max_scored_cursor(term, scorer), term);
        });
    std::sort(lookup_cursors.begin(), lookup_cursors.end(), [](auto&& lhs, auto&& rhs) {
        return lhs.max_score() > rhs.max_score();
    });

    auto term_to_position = [&] {
        std::unordered_map<TermId, std::uint32_t> term_to_position;
        std::uint32_t position = 0;
        for (auto&& cursor : essential_unigram_cursors) {
            term_to_position[cursor.label()] = position++;
        }
        for (auto&& cursor : lookup_cursors) {
            term_to_position[cursor.label()] = position++;
        }
        return term_to_position;
    }();

    auto merged_unigrams = v1::union_merge(
        essential_unigram_cursors, initial_payload, [&](auto& acc, auto& cursor, auto idx) {
            if constexpr (not std::is_void_v<Inspect>) {
                inspect->posting();
            }
            acc[idx] = cursor.payload();
            return acc;
        });

    std::vector<bigram_cursor_type> essential_bigram_cursors;
    for (auto [left, right] : essential_bigrams) {
        auto cursor = index.scored_bigram_cursor(left, right, scorer);
        if (not cursor) {
            throw std::runtime_error(fmt::format("Bigram not found: <{}, {}>", left, right));
        }
        essential_bigram_cursors.push_back(
            label(cursor.take().value(),
                  std::make_pair(term_to_position[left], term_to_position[right])));
    }

    auto merged_bigrams = v1::union_merge(std::move(essential_bigram_cursors),
                                          initial_payload,
                                          [&](auto& acc, auto& cursor, auto /* bigram_idx */) {
                                              if constexpr (not std::is_void_v<Inspect>) {
                                                  inspect->posting();
                                              }
                                              auto payload = cursor.payload();
                                              acc[cursor.label().first] = std::get<0>(payload);
                                              acc[cursor.label().second] = std::get<1>(payload);
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

    auto lookup_cursors_upper_bound = std::accumulate(
        lookup_cursors.begin(), lookup_cursors.end(), 0.0F, [](auto acc, auto&& cursor) {
            return acc + cursor.max_score();
        });

    auto next_lookup =
        precompute_next_lookup(essential_unigrams.size(), lookup_cursors.size(), [&] {
            std::vector<std::vector<std::uint32_t>> mapping(term_ids.size());
            for (auto&& cursor : essential_bigram_cursors) {
                auto [left, right] = cursor.label();
                mapping[left].push_back(right);
                mapping[right].push_back(left);
            }
            return mapping;
        }());
    auto mus = [&] {
        std::vector<float> mus((term_count + 1) * (1U << term_count), 0.0);
        for (auto term_idx = term_count; term_idx + 1 >= 1; term_idx -= 1) {
            for (std::uint32_t j = (1U << term_count) - 1; j + 1 >= 1; j -= 1) {
                auto state = (term_idx << term_count) + j;
                auto nt = next_lookup[state];
                if (nt == -1) {
                    mus[state] = 0.0F;
                } else {
                    auto a = lookup_cursors[nt - essential_unigrams.size()].max_score()
                             + mus[((nt + 1) << term_count) + (j | (1 << nt))];
                    auto b = mus[((term_idx + 1) << term_count) + j];
                    mus[state] = std::max(a, b);
                }
            }
        }
        return mus;
    }();

    auto const state_mask = (1U << term_count) - 1;

    v1::for_each(merged, [&](auto& cursor) {
        if constexpr (not std::is_void_v<Inspect>) {
            inspect->document();
        }
        auto docid = cursor.value();
        auto scores = cursor.payload();

        // auto score = std::accumulate(scores.begin(), scores.end(), 0.0F, std::plus{});
        float score = 0.0F;
        std::uint32_t state = essential_unigrams.size() << term_count;
        for (auto pos = 0U; pos < term_count; pos += 1) {
            if (scores[pos] > 0) {
                score += scores[pos];
                state |= 1U << pos;
            }
        }

        assert(state >= 0 && state < next_lookup.size());
        auto next_idx = next_lookup[state];
        while (next_idx >= 0 && topk.would_enter(score + mus[state])) {
            auto lookup_idx = next_idx - essential_unigrams.size();
            assert(lookup_idx >= 0 && lookup_idx < lookup_cursors.size());
            auto&& lookup_cursor = lookup_cursors[lookup_idx];
            lookup_cursor.advance_to_geq(docid);
            if constexpr (not std::is_void_v<Inspect>) {
                inspect->lookup();
            }
            if (lookup_cursor.value() == docid) {
                score += lookup_cursor.payload();
                state |= (1U << next_idx);
            }
            state = (state & state_mask) + ((next_idx + 1) << term_count);
            next_idx = next_lookup[state];
        }
        if constexpr (not std::is_void_v<Inspect>) {
            if (topk.insert(score, docid)) {
                inspect->insert();
            }
        } else {
            topk.insert(score, docid);
        }
    });
    return topk;
}

template <typename Index, typename Scorer>
struct InspectUnionLookup : Inspect<Index,
                                    Scorer,
                                    InspectPostings,
                                    InspectDocuments,
                                    InspectLookups,
                                    InspectInserts,
                                    InspectEssential> {

    InspectUnionLookup(Index const& index, Scorer const& scorer)
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
        if (query.selections()->bigrams.empty()) {
            unigram_union_lookup(query, index, std::move(topk), scorer, this);
        } else if (query.get_term_ids().size() > 8) {
            maxscore_union_lookup(query, index, std::move(topk), scorer, this);
        } else {
            union_lookup(query, index, std::move(topk), scorer, this);
        }
    }
};

template <typename Index, typename Scorer>
struct InspectUnionLookupPlus : Inspect<Index,
                                        Scorer,
                                        InspectPostings,
                                        InspectDocuments,
                                        InspectLookups,
                                        InspectInserts,
                                        InspectEssential> {

    InspectUnionLookupPlus(Index const& index, Scorer const& scorer)
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
        if (query.selections()->bigrams.empty()) {
            unigram_union_lookup(query, index, std::move(topk), scorer, this);
        } else if (query.get_term_ids().size() > 8) {
            maxscore_union_lookup(query, index, std::move(topk), scorer, this);
        } else {
            union_lookup_plus(query, index, std::move(topk), scorer, this);
        }
    }
};

using LookupUnionComponent = InspectMany<InspectPostings,
                                         InspectDocuments,
                                         InspectLookups,
                                         InspectInserts,
                                         InspectEssential>;

template <typename Index, typename Scorer>
struct InspectLookupUnion : Inspect<Index, Scorer, InspectPartitioned<LookupUnionComponent>> {

    InspectLookupUnion(Index const& index, Scorer scorer)
        : Inspect<Index, Scorer, InspectPartitioned<LookupUnionComponent>>(index, scorer)
    {
    }

    void run(Query const& query, Index const& index, Scorer const& scorer, topk_queue topk) override
    {
        if (query.selections()->bigrams.empty()) {
            unigram_union_lookup(query,
                                 index,
                                 std::move(topk),
                                 scorer,
                                 InspectPartitioned<LookupUnionComponent>::first());
        } else {
            lookup_union(query,
                         index,
                         std::move(topk),
                         scorer,
                         InspectPartitioned<LookupUnionComponent>::first(),
                         InspectPartitioned<LookupUnionComponent>::second());
        }
    }
};

template <typename Index, typename Scorer>
struct InspectLookupUnionEaat : Inspect<Index, Scorer, InspectPartitioned<LookupUnionComponent>> {

    InspectLookupUnionEaat(Index const& index, Scorer scorer)
        : Inspect<Index, Scorer, InspectPartitioned<LookupUnionComponent>>(index, scorer)
    {
    }

    void run(Query const& query, Index const& index, Scorer const& scorer, topk_queue topk) override
    {
        if (query.selections()->bigrams.empty()) {
            unigram_union_lookup(query,
                                 index,
                                 std::move(topk),
                                 scorer,
                                 InspectPartitioned<LookupUnionComponent>::first());
        } else {
            lookup_union_eaat(query,
                              index,
                              std::move(topk),
                              scorer,
                              InspectPartitioned<LookupUnionComponent>::first(),
                              InspectPartitioned<LookupUnionComponent>::second());
        }
    }
};

} // namespace pisa::v1
