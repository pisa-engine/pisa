#pragma once

#include <algorithm>
#include <numeric>
#include <vector>

#include <gsl/span>
#include <range/v3/view/iota.hpp>

#include "v1/algorithm.hpp"
#include "v1/cursor_accumulator.hpp"
#include "v1/inspect_query.hpp"
#include "v1/query.hpp"

namespace pisa::v1 {

template <typename CursorContainer,
          typename Payload,
          typename AccumulateFn,
          typename ThresholdFn,
          typename Inspect = void>
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
                           Inspect* inspect)
        : m_cursors(std::move(cursors)),
          m_upper_bounds(m_cursors.size()),
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
        std::sort(m_cursors.begin(), m_cursors.end(), [](auto&& lhs, auto&& rhs) {
            return lhs.max_score() < rhs.max_score();
        });

        m_upper_bounds[0] = m_cursors[0].max_score();
        for (size_t i = 1; i < m_cursors.size(); ++i) {
            m_upper_bounds[i] = m_upper_bounds[i - 1] + m_cursors[i].max_score();
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
        while (m_non_essential_count < m_cursors.size()
               && not m_above_threshold(m_upper_bounds[m_non_essential_count])) {
            m_non_essential_count += 1;
        }
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
                    m_current_payload = m_accumulate(m_current_payload, cursor);
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
                    m_current_payload = m_accumulate(m_current_payload, cursor);
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

    Inspect* m_inspect;
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
          typename Inspect>
auto join_maxscore(CursorContainer cursors,
                   Payload init,
                   AccumulateFn accumulate,
                   ThresholdFn threshold,
                   Inspect* inspect)
{
    return MaxScoreJoin<CursorContainer, Payload, AccumulateFn, ThresholdFn, Inspect>(
        std::move(cursors), std::move(init), std::move(accumulate), std::move(threshold), inspect);
}

template <typename Index, typename Scorer, typename Inspect = void>
auto maxscore(Query const& query,
              Index const& index,
              topk_queue topk,
              Scorer&& scorer,
              Inspect* inspect = nullptr)
{
    auto const& term_ids = query.get_term_ids();
    if (term_ids.empty()) {
        return topk;
    }
    using cursor_type = decltype(index.max_scored_cursor(0, scorer));
    using value_type = decltype(index.max_scored_cursor(0, scorer).value());

    auto cursors = index.max_scored_cursors(gsl::make_span(term_ids), scorer);
    if (query.threshold()) {
        topk.set_threshold(*query.threshold());
    }
    auto joined = join_maxscore(
        std::move(cursors),
        0.0F,
        accumulators::Add{},
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
struct InspectMaxScore : Inspect<Index,
                                 Scorer,
                                 InspectPostings,
                                 InspectDocuments,
                                 InspectLookups,
                                 InspectInserts,
                                 InspectEssential> {

    InspectMaxScore(Index const& index, Scorer const& scorer)
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
        maxscore(query, index, std::move(topk), scorer, this);
    }
};

} // namespace pisa::v1
