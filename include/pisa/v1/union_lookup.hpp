#pragma once

#include <algorithm>

#include <range/v3/range/conversion.hpp>
#include <range/v3/view/filter.hpp>
#include <range/v3/view/reverse.hpp>
#include <range/v3/view/set_algorithm.hpp>

#include "v1/algorithm.hpp"
#include "v1/cursor/transform.hpp"
#include "v1/cursor_accumulator.hpp"
#include "v1/query.hpp"

namespace pisa::v1 {

template <typename CursorContainer,
          typename Payload,
          typename AccumulateFn,
          typename ThresholdFn,
          typename Inspect = void>
struct UnionLookupJoin {
    using cursor_type = typename CursorContainer::value_type;
    using payload_type = Payload;
    using value_type = std::decay_t<decltype(*std::declval<cursor_type>())>;

    using iterator_category =
        typename std::iterator_traits<typename CursorContainer::iterator>::iterator_category;
    static_assert(std::is_base_of<std::random_access_iterator_tag, iterator_category>(),
                  "cursors must be stored in a random access container");

    UnionLookupJoin(CursorContainer cursors,
                    std::vector<std::size_t> cursor_idx,
                    std::vector<payload_type> upper_bounds,
                    std::size_t non_essential_count,
                    Payload init,
                    AccumulateFn accumulate,
                    ThresholdFn above_threshold)
        : m_cursors(std::move(cursors)),
          m_cursor_idx(std::move(cursor_idx)),
          m_upper_bounds(std::move(upper_bounds)),
          m_non_essential_count(non_essential_count),
          m_init(std::move(init)),
          m_accumulate(std::move(accumulate)),
          m_above_threshold(std::move(above_threshold)),
          m_size(std::nullopt)
    {
        initialize();
    }

    UnionLookupJoin(CursorContainer cursors,
                    std::vector<std::size_t> cursor_idx,
                    std::vector<payload_type> upper_bounds,
                    std::size_t non_essential_count,
                    Payload init,
                    AccumulateFn accumulate,
                    ThresholdFn above_threshold,
                    Inspect* inspect)
        : m_cursors(std::move(cursors)),
          m_cursor_idx(std::move(cursor_idx)),
          m_upper_bounds(std::move(upper_bounds)),
          m_non_essential_count(non_essential_count),
          m_init(std::move(init)),
          m_accumulate(std::move(accumulate)),
          m_above_threshold(std::move(above_threshold)),
          m_size(std::nullopt),
          m_inspect(inspect)
    {
        initialize();
    }

    void initialize()
    {
        if (m_cursors.empty()) {
            m_current_value = sentinel();
            m_current_payload = m_init;
        }
        m_next_docid = min_value(m_cursors);
        m_sentinel = min_sentinel(m_cursors);
        advance();
    }

    [[nodiscard]] constexpr auto operator*() const noexcept -> value_type
    {
        return m_current_value;
    }
    [[nodiscard]] constexpr auto value() const noexcept -> value_type { return m_current_value; }
    [[nodiscard]] constexpr auto payload() const noexcept -> Payload const&
    {
        return m_current_payload;
    }
    [[nodiscard]] constexpr auto sentinel() const noexcept -> std::uint32_t { return m_sentinel; }

    constexpr void advance()
    {
        bool exit = false;
        while (not exit) {
            if (PISA_UNLIKELY(m_non_essential_count == m_cursors.size()
                              || m_next_docid >= sentinel())) {
                m_current_value = sentinel();
                m_current_payload = m_init;
                return;
            }
            m_current_payload = m_init;
            m_current_value = std::exchange(m_next_docid, sentinel());

            if constexpr (not std::is_void_v<Inspect>) {
                m_inspect->document();
            }

            for (auto sorted_position = m_non_essential_count; sorted_position < m_cursors.size();
                 sorted_position += 1) {

                auto& cursor = m_cursors[sorted_position];
                if (cursor.value() == m_current_value) {
                    if constexpr (not std::is_void_v<Inspect>) {
                        m_inspect->posting();
                    }
                    m_current_payload =
                        m_accumulate(m_current_payload, cursor, m_cursor_idx[sorted_position]);
                    cursor.advance();
                }
                if (auto docid = cursor.value(); docid < m_next_docid) {
                    m_next_docid = docid;
                }
            }

            exit = true;
            for (auto sorted_position = m_non_essential_count - 1; sorted_position + 1 > 0;
                 sorted_position -= 1) {
                if (not m_above_threshold(m_current_payload + m_upper_bounds[sorted_position])) {
                    exit = false;
                    break;
                }
                auto& cursor = m_cursors[sorted_position];
                cursor.advance_to_geq(m_current_value);
                if constexpr (not std::is_void_v<Inspect>) {
                    m_inspect->lookup();
                }
                if (cursor.value() == m_current_value) {
                    m_current_payload =
                        m_accumulate(m_current_payload, cursor, m_cursor_idx[sorted_position]);
                }
            }
        }
    }

    [[nodiscard]] constexpr auto position() const noexcept -> std::size_t; // TODO(michal)
    [[nodiscard]] constexpr auto empty() const noexcept -> bool
    {
        return m_current_value >= sentinel();
    }

   private:
    CursorContainer m_cursors;
    std::vector<std::size_t> m_cursor_idx;
    std::vector<payload_type> m_upper_bounds;
    std::size_t m_non_essential_count;
    payload_type m_init;
    AccumulateFn m_accumulate;
    ThresholdFn m_above_threshold;
    std::optional<std::size_t> m_size;

    value_type m_current_value{};
    value_type m_sentinel{};
    payload_type m_current_payload{};
    std::uint32_t m_next_docid{};
    payload_type m_previous_threshold{};

    Inspect* m_inspect;
};

template <typename CursorContainer, typename Payload, typename AccumulateFn, typename ThresholdFn>
auto join_union_lookup(CursorContainer cursors,
                       std::vector<std::size_t> cursor_idx,
                       std::vector<Payload> upper_bounds,
                       std::size_t non_essential_count,
                       Payload init,
                       AccumulateFn accumulate,
                       ThresholdFn threshold)
{
    return UnionLookupJoin<CursorContainer, Payload, AccumulateFn, ThresholdFn, void>(
        std::move(cursors),
        std::move(cursor_idx),
        std::move(upper_bounds),
        non_essential_count,
        std::move(init),
        std::move(accumulate),
        std::move(threshold));
}

template <typename CursorContainer,
          typename Payload,
          typename AccumulateFn,
          typename ThresholdFn,
          typename Inspect>
auto join_union_lookup(CursorContainer cursors,
                       std::vector<std::size_t> cursor_idx,
                       std::vector<Payload> upper_bounds,
                       std::size_t non_essential_count,
                       Payload init,
                       AccumulateFn accumulate,
                       ThresholdFn threshold,
                       Inspect* inspect)
{
    return UnionLookupJoin<CursorContainer, Payload, AccumulateFn, ThresholdFn, Inspect>(
        std::move(cursors),
        std::move(cursor_idx),
        std::move(upper_bounds),
        non_essential_count,
        std::move(init),
        std::move(accumulate),
        std::move(threshold),
        inspect);
}

namespace detail {
    template <typename Cursors, typename UpperBounds, typename Inspect = void>
    auto unigram_union_lookup(Cursors cursors,
                              UpperBounds upper_bounds,
                              std::size_t non_essential_count,
                              topk_queue topk,
                              [[maybe_unused]] Inspect* inspect = nullptr)
    {
        auto merged_essential =
            v1::union_merge(gsl::make_span(cursors).subspan(non_essential_count),
                            0.0F,
                            [&](auto& acc, auto& cursor, auto /*term_idx*/) {
                                if constexpr (not std::is_void_v<Inspect>) {
                                    inspect->posting();
                                }
                                acc += cursor.payload();
                                return acc;
                            });

        v1::for_each(merged_essential, [&](auto&& cursor) {
            if constexpr (not std::is_void_v<Inspect>) {
                inspect->document();
            }
            auto docid = cursor.value();
            auto score = cursor.payload();
            for (auto lookup_cursor_idx = non_essential_count - 1; lookup_cursor_idx + 1 > 0;
                 lookup_cursor_idx -= 1) {
                if (not topk.would_enter(score + upper_bounds[lookup_cursor_idx])) {
                    return;
                }
                auto& lookup_cursor = cursors[lookup_cursor_idx];
                lookup_cursor.advance_to_geq(docid);
                if constexpr (not std::is_void_v<Inspect>) {
                    inspect->lookup();
                }
                if (lookup_cursor.value() == docid) {
                    score += lookup_cursor.payload();
                }
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
} // namespace detail

template <typename Index, typename Scorer, typename Inspect = void>
auto unigram_union_lookup(Query const& query,
                          Index const& index,
                          topk_queue topk,
                          Scorer&& scorer,
                          [[maybe_unused]] Inspect* inspect = nullptr)
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

    // We're not interested in these being correct
    std::vector<std::size_t> cursor_idx(cursors.size());
    std::iota(cursor_idx.begin(), cursor_idx.end(), 0);

    auto accumulate = [](auto& score, auto& cursor, auto /* term_position */) {
        score += cursor.payload();
        return score;
    };
    auto joined = join_union_lookup(
        std::move(cursors),
        std::move(cursor_idx),
        std::move(upper_bounds),
        non_essential_count,
        payload_type{},
        accumulate,
        [&](auto score) { return topk.would_enter(score); },
        inspect);
    v1::for_each(joined, [&](auto& cursor) {
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
    std::vector<std::size_t> cursor_idx(cursors.size());
    std::iota(cursor_idx.begin(), cursor_idx.end(), 0);
    std::sort(cursor_idx.begin(), cursor_idx.end(), [&](auto&& lhs, auto&& rhs) {
        return cursors[lhs].max_score() < cursors[rhs].max_score();
    });
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

    auto accumulate = [](auto& score, auto& cursor, auto /* term_position */) {
        score += cursor.payload();
        return score;
    };
    auto joined = join_union_lookup(
        std::move(cursors),
        std::move(cursor_idx),
        std::move(upper_bounds),
        non_essential_count,
        payload_type{},
        accumulate,
        [&](auto score) { return topk.would_enter(score); },
        inspect);
    v1::for_each(joined, [&](auto& cursor) {
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
struct BaseUnionLookupInspect {
    BaseUnionLookupInspect(Index const& index, Scorer scorer)
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
struct MaxscoreUnionLookupInspect : public BaseUnionLookupInspect<Index, Scorer> {
    MaxscoreUnionLookupInspect(Index const& index, Scorer scorer)
        : BaseUnionLookupInspect<Index, Scorer>(index, std::move(scorer))
    {
    }
    void run(Query const& query, Index const& index, Scorer& scorer, topk_queue topk) override
    {
        maxscore_union_lookup(query, index, std::move(topk), scorer, this);
    }
};

template <typename Index, typename Scorer>
struct UnigramUnionLookupInspect : public BaseUnionLookupInspect<Index, Scorer> {
    UnigramUnionLookupInspect(Index const& index, Scorer scorer)
        : BaseUnionLookupInspect<Index, Scorer>(index, std::move(scorer))
    {
    }
    void run(Query const& query, Index const& index, Scorer& scorer, topk_queue topk) override
    {
        unigram_union_lookup(query, index, std::move(topk), scorer, this);
    }
};

template <typename Index, typename Scorer>
struct UnionLookupInspect : public BaseUnionLookupInspect<Index, Scorer> {
    UnionLookupInspect(Index const& index, Scorer scorer)
        : BaseUnionLookupInspect<Index, Scorer>(index, std::move(scorer))
    {
    }
    void run(Query const& query, Index const& index, Scorer& scorer, topk_queue topk) override
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
struct LookupUnionInspector : public BaseUnionLookupInspect<Index, Scorer> {
    LookupUnionInspector(Index const& index, Scorer scorer)
        : BaseUnionLookupInspect<Index, Scorer>(index, std::move(scorer))
    {
    }
    void run(Query const& query, Index const& index, Scorer& scorer, topk_queue topk) override
    {
        if (query.selections()->bigrams.empty()) {
            unigram_union_lookup(query, index, std::move(topk), scorer, this);
        } else {
            lookup_union(query, index, std::move(topk), scorer, this);
        }
    }
};

// template <typename Index, typename Scorer>
// struct TwoPhaseUnionLookupInspect : public BaseUnionLookupInspect<Index, Scorer> {
//    TwoPhaseUnionLookupInspect(Index const& index, Scorer scorer)
//        : BaseUnionLookupInspect<Index, Scorer>(index, std::move(scorer))
//    {
//    }
//    void run(Query const& query, Index const& index, Scorer& scorer, topk_queue topk) override
//    {
//        // if (query.get_term_ids().size() > 8) {
//        maxscore_union_lookup(query, index, std::move(topk), scorer, this);
//        //} else {
//        //    two_phase_union_lookup(query, index, std::move(topk), scorer, this);
//        //}
//    }
//};

/// This one assumes that terms do not repeat in essential lists and intersections.
template <typename Index, typename Scorer, typename Inspect = void>
auto disjoint_union_lookup(Query const& query,
                           Index const& index,
                           topk_queue topk,
                           Scorer&& scorer,
                           Inspect* inspect = nullptr)
{
    using bigram_cursor_type = std::decay_t<decltype(*index.scored_bigram_cursor(0, 0, scorer))>;

    auto const& term_ids = query.get_term_ids();
    if (term_ids.empty()) {
        return topk;
    }

    auto threshold = query.get_threshold();
    topk.set_threshold(threshold);

    auto const& selections = query.get_selections();
    auto const& essential_unigrams = selections.unigrams;
    auto const& essential_bigrams = selections.bigrams;

    auto const non_essential_terms = [&]() {
        auto all_essential_terms = essential_unigrams;
        for (auto [left, right] : essential_bigrams) {
            all_essential_terms.push_back(left);
            all_essential_terms.push_back(right);
        }
        ranges::sort(all_essential_terms);
        ranges::actions::unique(all_essential_terms);
        return ranges::views::set_difference(term_ids, all_essential_terms) | ranges::to_vector;
    }();

    auto essential_unigram_cursors =
        index.scored_cursors(gsl::make_span(essential_unigrams), scorer);
    auto merged_unigrams = v1::union_merge(
        essential_unigram_cursors, 0.0F, pisa::v1::accumulate::InspectAdd<Inspect>(inspect));

    std::vector<bigram_cursor_type> essential_bigram_cursors;
    for (auto [left, right] : essential_bigrams) {
        auto cursor = index.scored_bigram_cursor(left, right, scorer);
        if (not cursor) {
            throw std::runtime_error(fmt::format("Bigram not found: <{}, {}>", left, right));
        }
        essential_bigram_cursors.push_back(cursor.take().value());
    }

    auto merged_bigrams =
        v1::union_merge(std::move(essential_bigram_cursors),
                        0.0F,
                        [&](auto& acc, auto& cursor, [[maybe_unused]] auto bigram_idx) {
                            if constexpr (not std::is_void_v<Inspect>) {
                                inspect->posting();
                            }
                            auto payload = cursor.payload();
                            acc += std::get<0>(payload) + std::get<1>(payload);
                            return acc;
                        });
    auto merged = v1::variadic_union_merge(
        0.0F,
        std::make_tuple(std::move(merged_unigrams), std::move(merged_bigrams)),
        std::make_tuple(pisa::v1::accumulate::Add{}, pisa::v1::accumulate::Add{}));

    auto lookup_cursors = index.max_scored_cursors(non_essential_terms, scorer);
    std::sort(lookup_cursors.begin(), lookup_cursors.end(), [](auto&& lhs, auto&& rhs) {
        return lhs.max_score() > rhs.max_score();
    });
    auto lookup_cursors_upper_bound = std::accumulate(
        lookup_cursors.begin(), lookup_cursors.end(), 0.0F, [](auto acc, auto&& cursor) {
            return acc + cursor.max_score();
        });

    v1::for_each(merged, [&](auto& cursor) {
        if constexpr (not std::is_void_v<Inspect>) {
            inspect->document();
        }
        auto docid = cursor.value();
        auto score = cursor.payload();
        auto upper_bound = score + lookup_cursors_upper_bound;
        for (auto& lookup_cursor : lookup_cursors) {
            if (not topk.would_enter(upper_bound)) {
                return;
            }
            lookup_cursor.advance_to_geq(docid);
            if constexpr (not std::is_void_v<Inspect>) {
                inspect->lookup();
            }
            if (PISA_UNLIKELY(lookup_cursor.value() == docid)) {
                auto partial_score = lookup_cursor.payload();
                score += partial_score;
                upper_bound += partial_score;
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

template <typename Cursor,
          typename LookupCursor,
          typename AboveThresholdFn,
          typename Inspector = void>
struct LookupTransform {

    LookupTransform(std::vector<LookupCursor> lookup_cursors,
                    std::vector<float> upper_bounds,
                    float lookup_cursors_upper_bound,
                    AboveThresholdFn above_threshold,
                    Inspector* inspect = nullptr)
        : m_lookup_cursors(std::move(lookup_cursors)),
          m_upper_bounds(std::move(upper_bounds)),
          m_lookup_cursors_upper_bound(lookup_cursors_upper_bound),
          m_above_threshold(std::move(above_threshold)),
          m_inspect(inspect)
    {
    }

    auto operator()(Cursor& cursor)
    {
        if constexpr (not std::is_void_v<Inspector>) {
            m_inspect->document();
            m_inspect->posting();
        }
        auto docid = cursor.value();
        auto scores = cursor.payload();
        float score = std::get<0>(scores) + std::get<1>(scores);
        auto upper_bound = score + m_lookup_cursors_upper_bound;
        for (auto& lookup_cursor : m_lookup_cursors) {
            if (not m_above_threshold(upper_bound)) {
                return score;
            }
            lookup_cursor.advance_to_geq(docid);
            if constexpr (not std::is_void_v<Inspector>) {
                m_inspect->lookup();
            }
            if (PISA_UNLIKELY(lookup_cursor.value() == docid)) {
                auto partial_score = lookup_cursor.payload();
                score += partial_score;
                upper_bound += partial_score;
            }
            upper_bound -= lookup_cursor.max_score();
        }
        return score;
    }

   private:
    std::vector<LookupCursor> m_lookup_cursors;
    std::vector<float> m_upper_bounds;
    float m_lookup_cursors_upper_bound;
    AboveThresholdFn m_above_threshold;
    Inspector* m_inspect;
};

template <typename Index, typename Scorer, typename Inspector = void>
auto lookup_union(Query const& query,
                  Index const& index,
                  topk_queue topk,
                  Scorer&& scorer,
                  Inspector* inspector = nullptr)
{
    auto const& term_ids = query.get_term_ids();
    if (term_ids.empty()) {
        return topk;
    }

    auto threshold = query.get_threshold();
    auto const& selections = query.get_selections();

    using bigram_cursor_type = std::decay_t<decltype(*index.scored_bigram_cursor(0, 0, scorer))>;
    using lookup_cursor_type = std::decay_t<decltype(index.max_scored_cursor(0, scorer))>;

    auto& essential_unigrams = selections.unigrams;
    auto& essential_bigrams = selections.bigrams;

    auto non_essential_terms =
        ranges::views::set_difference(term_ids, essential_unigrams) | ranges::to_vector;

    topk.set_threshold(threshold);
    auto is_above_threshold = [&](auto score) { return topk.would_enter(score); };

    auto unigram_cursor = [&]() {
        auto cursors = index.max_scored_cursors(gsl::make_span(non_essential_terms), scorer);
        ranges::sort(cursors,
                     [](auto&& lhs, auto&& rhs) { return lhs.max_score() > rhs.max_score(); });
        auto non_essential_count = cursors.size();
        for (auto term : essential_unigrams) {
            cursors.push_back(index.max_scored_cursor(term, scorer));
        }

        std::vector<float> upper_bounds(cursors.size());
        upper_bounds[0] = cursors[0].max_score();
        for (size_t i = 1; i < cursors.size(); ++i) {
            upper_bounds[i] = upper_bounds[i - 1] + cursors[i].max_score();
        }

        // We're not interested in these being correct
        std::vector<std::size_t> cursor_idx(cursors.size());
        std::iota(cursor_idx.begin(), cursor_idx.end(), 0);

        return join_union_lookup(std::move(cursors),
                                 std::move(cursor_idx),
                                 std::move(upper_bounds),
                                 non_essential_count,
                                 0.0F,
                                 pisa::v1::accumulate::Add{},
                                 is_above_threshold,
                                 inspector);
    }();

    using lookup_transform_type = LookupTransform<bigram_cursor_type,
                                                  lookup_cursor_type,
                                                  decltype(is_above_threshold),
                                                  Inspector>;
    using transform_payload_cursor_type =
        TransformPayloadCursor<bigram_cursor_type, lookup_transform_type>;
    std::vector<transform_payload_cursor_type> bigram_cursors;
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

        std::vector<float> upper_bounds(lookup_cursors.size());
        if (not lookup_cursors.empty()) {
            upper_bounds[0] = lookup_cursors[0].max_score();
            for (size_t i = 1; i < lookup_cursors.size(); ++i) {
                upper_bounds[i] = upper_bounds[i - 1] + lookup_cursors[i].max_score();
            }
        }
        auto lookup_cursors_upper_bound = std::accumulate(
            lookup_cursors.begin(), lookup_cursors.end(), 0.0F, [](auto acc, auto&& cursor) {
                return acc + cursor.max_score();
            });

        bigram_cursors.emplace_back(std::move(*cursor.take()),
                                    lookup_transform_type(std::move(lookup_cursors),
                                                          std::move(upper_bounds),
                                                          lookup_cursors_upper_bound,
                                                          is_above_threshold,
                                                          inspector));
    }

    auto accumulate = [&](float acc, auto& cursor, [[maybe_unused]] auto idx) {
        return std::max(acc, cursor.payload());
    };
    auto bigram_cursor = union_merge(std::move(bigram_cursors), 0.0F, accumulate);
    auto merged = v1::variadic_union_merge(
        0.0F,
        std::make_tuple(std::move(unigram_cursor), std::move(bigram_cursor)),
        std::make_tuple(accumulate, accumulate));

    v1::for_each(merged, [&](auto&& cursor) {
        if constexpr (not std::is_void_v<Inspector>) {
            if (topk.insert(cursor.payload(), cursor.value())) {
                inspector->insert();
            }
        } else {
            topk.insert(cursor.payload(), cursor.value());
        }
    });
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

    std::array<float, 8> initial_payload{
        0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0}; //, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0};

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

} // namespace pisa::v1
