#pragma once

#include <algorithm>
#include <numeric>
#include <vector>

#include <gsl/span>
#include <range/v3/view/iota.hpp>

#include "v1/algorithm.hpp"
#include "v1/query.hpp"

namespace pisa::v1 {

template <typename CursorContainer,
          typename Payload,
          typename AccumulateFn,
          typename ThresholdFn,
          typename Analyzer = void>
struct MaxScoreJoin {
    using cursor_type = typename CursorContainer::value_type;
    using payload_type = Payload;
    using value_type = std::decay_t<decltype(*std::declval<cursor_type>())>;

    using iterator_category =
        typename std::iterator_traits<typename CursorContainer::iterator>::iterator_category;
    static_assert(std::is_base_of<std::random_access_iterator_tag, iterator_category>(),
                  "cursors must be stored in a random access container");

    constexpr MaxScoreJoin(CursorContainer cursors,
                           Payload init,
                           AccumulateFn accumulate,
                           ThresholdFn above_threshold)
        : m_cursors(std::move(cursors)),
          m_sorted_cursors(m_cursors.size()),
          m_cursor_idx(m_cursors.size()),
          m_upper_bounds(m_cursors.size()),
          m_init(std::move(init)),
          m_accumulate(std::move(accumulate)),
          m_above_threshold(std::move(above_threshold)),
          m_size(std::nullopt)
    {
        initialize();
    }

    constexpr MaxScoreJoin(CursorContainer cursors,
                           Payload init,
                           AccumulateFn accumulate,
                           ThresholdFn above_threshold,
                           Analyzer* analyzer)
        : m_cursors(std::move(cursors)),
          m_sorted_cursors(m_cursors.size()),
          m_cursor_idx(m_cursors.size()),
          m_upper_bounds(m_cursors.size()),
          m_init(std::move(init)),
          m_accumulate(std::move(accumulate)),
          m_above_threshold(std::move(above_threshold)),
          m_size(std::nullopt),
          m_analyzer(analyzer)
    {
        initialize();
    }

    void initialize()
    {
        if (m_cursors.empty()) {
            m_current_value = sentinel();
            m_current_payload = m_init;
        }
        std::transform(m_cursors.begin(),
                       m_cursors.end(),
                       m_sorted_cursors.begin(),
                       [](auto&& cursor) { return &cursor; });
        std::sort(m_sorted_cursors.begin(), m_sorted_cursors.end(), [](auto&& lhs, auto&& rhs) {
            return lhs->max_score() < rhs->max_score();
        });
        std::iota(m_cursor_idx.begin(), m_cursor_idx.end(), 0);
        std::sort(m_cursor_idx.begin(), m_cursor_idx.end(), [this](auto&& lhs, auto&& rhs) {
            return m_cursors[lhs].max_score() < m_cursors[rhs].max_score();
        });

        m_upper_bounds[0] = m_sorted_cursors[0]->max_score();
        for (size_t i = 1; i < m_sorted_cursors.size(); ++i) {
            m_upper_bounds[i] = m_upper_bounds[i - 1] + m_sorted_cursors[i]->max_score();
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

            if constexpr (not std::is_void_v<Analyzer>) {
                m_analyzer->document();
            }

            for (auto sorted_position = m_non_essential_count;
                 sorted_position < m_sorted_cursors.size();
                 sorted_position += 1) {

                auto& cursor = m_sorted_cursors[sorted_position];
                if (cursor->value() == m_current_value) {
                    if constexpr (not std::is_void_v<Analyzer>) {
                        m_analyzer->posting();
                    }
                    m_current_payload =
                        m_accumulate(m_current_payload, *cursor, m_cursor_idx[sorted_position]);
                    cursor->advance();
                }
                if (auto docid = cursor->value(); docid < m_next_docid) {
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
                auto& cursor = m_sorted_cursors[sorted_position];
                cursor->advance_to_geq(m_current_value);
                if constexpr (not std::is_void_v<Analyzer>) {
                    m_analyzer->lookup();
                }
                if (cursor->value() == m_current_value) {
                    m_current_payload =
                        m_accumulate(m_current_payload, *cursor, m_cursor_idx[sorted_position]);
                }
            }
        }

        while (m_non_essential_count < m_cursors.size()
               && not m_above_threshold(m_upper_bounds[m_non_essential_count])) {
            m_non_essential_count += 1;
        }
    }

    [[nodiscard]] constexpr auto position() const noexcept -> std::size_t; // TODO(michal)
    [[nodiscard]] constexpr auto empty() const noexcept -> bool
    {
        return m_current_value >= sentinel();
    }

   private:
    CursorContainer m_cursors;
    std::vector<cursor_type*> m_sorted_cursors;
    std::vector<std::size_t> m_cursor_idx;
    std::vector<payload_type> m_upper_bounds;
    payload_type m_init;
    AccumulateFn m_accumulate;
    ThresholdFn m_above_threshold;
    std::optional<std::size_t> m_size;

    value_type m_current_value{};
    value_type m_sentinel{};
    payload_type m_current_payload{};
    std::uint32_t m_next_docid{};
    std::size_t m_non_essential_count = 0;
    payload_type m_previous_threshold{};

    Analyzer* m_analyzer;
};

template <typename CursorContainer, typename Payload, typename AccumulateFn, typename ThresholdFn>
auto join_maxscore(CursorContainer cursors,
                   Payload init,
                   AccumulateFn accumulate,
                   ThresholdFn threshold)
{
    return MaxScoreJoin<CursorContainer, Payload, AccumulateFn, ThresholdFn, void>(
        std::move(cursors), std::move(init), std::move(accumulate), std::move(threshold));
}

template <typename CursorContainer,
          typename Payload,
          typename AccumulateFn,
          typename ThresholdFn,
          typename Analyzer>
auto join_maxscore(CursorContainer cursors,
                   Payload init,
                   AccumulateFn accumulate,
                   ThresholdFn threshold,
                   Analyzer* analyzer)
{
    return MaxScoreJoin<CursorContainer, Payload, AccumulateFn, ThresholdFn, Analyzer>(
        std::move(cursors), std::move(init), std::move(accumulate), std::move(threshold), analyzer);
}

template <typename Index, typename Scorer>
auto maxscore(Query const& query, Index const& index, topk_queue topk, Scorer&& scorer)
{
    if (query.terms.empty()) {
        return topk;
    }
    using cursor_type = decltype(index.max_scored_cursor(0, scorer));
    using value_type = decltype(index.max_scored_cursor(0, scorer).value());

    std::vector<cursor_type> cursors;
    std::transform(query.terms.begin(),
                   query.terms.end(),
                   std::back_inserter(cursors),
                   [&](auto term) { return index.max_scored_cursor(term, scorer); });

    auto accumulate = [](auto& score, auto& cursor, auto /* term_position */) {
        score += cursor.payload();
        return score;
    };
    if (query.threshold) {
        topk.set_threshold(*query.threshold);
    }
    auto joined = join_maxscore(
        std::move(cursors), 0.0F, accumulate, [&](auto score) { return topk.would_enter(score); });
    v1::for_each(joined, [&](auto& cursor) { topk.insert(cursor.payload(), cursor.value()); });
    return topk;
}

template <typename Index, typename Scorer>
struct MaxscoreAnalyzer {
    MaxscoreAnalyzer(Index const& index, Scorer scorer)
        : m_index(index), m_scorer(std::move(scorer))
    {
        std::cout << fmt::format("documents\tpostings\tinserts\tlookups\n");
    }

    void operator()(Query const& query)
    {
        if (query.terms.empty()) {
            return;
        }
        using cursor_type = decltype(m_index.max_scored_cursor(0, m_scorer));
        using value_type = decltype(m_index.max_scored_cursor(0, m_scorer).value());

        m_current_documents = 0;
        m_current_postings = 0;
        m_current_lookups = 0;

        std::vector<cursor_type> cursors;
        std::transform(query.terms.begin(),
                       query.terms.end(),
                       std::back_inserter(cursors),
                       [&](auto term) { return m_index.max_scored_cursor(term, m_scorer); });

        std::size_t inserts = 0;
        topk_queue topk(query.k);
        auto initial_threshold = query.threshold.value_or(-1.0);
        topk.set_threshold(initial_threshold);
        auto joined = join_maxscore(
            std::move(cursors),
            0.0F,
            [&](auto& score, auto& cursor, auto /* term_position */) {
                score += cursor.payload();
                return score;
            },
            [&](auto score) { return topk.would_enter(score); },
            this);
        v1::for_each(joined, [&](auto& cursor) {
            if (topk.insert(cursor.payload(), cursor.value())) {
                inserts += 1;
            };
        });
        std::cout << fmt::format("{}\t{}\t{}\t{}\n",
                                 m_current_documents,
                                 m_current_postings,
                                 inserts,
                                 m_current_lookups);
        m_documents += m_current_documents;
        m_postings += m_current_postings;
        m_lookups += m_current_lookups;
        m_inserts += inserts;
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

   private:
    std::size_t m_current_documents = 0;
    std::size_t m_current_postings = 0;
    std::size_t m_current_lookups = 0;

    std::size_t m_documents = 0;
    std::size_t m_postings = 0;
    std::size_t m_lookups = 0;
    std::size_t m_inserts = 0;
    std::size_t m_count = 0;
    Index const& m_index;
    Scorer m_scorer;
};

} // namespace pisa::v1
