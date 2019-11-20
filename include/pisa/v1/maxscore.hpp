#pragma once

#include <algorithm>
#include <numeric>
#include <vector>

#include <gsl/span>
#include <range/v3/view/iota.hpp>

#include "v1/algorithm.hpp"
#include "v1/query.hpp"

namespace pisa::v1 {

template <typename CursorContainer, typename Payload, typename AccumulateFn, typename ThresholdFn>
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

            for (auto sorted_position = m_non_essential_count;
                 sorted_position < m_sorted_cursors.size();
                 sorted_position += 1) {

                auto& cursor = m_sorted_cursors[sorted_position];
                if (cursor->value() == m_current_value) {
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
};

template <typename CursorContainer, typename Payload, typename AccumulateFn, typename ThresholdFn>
auto join_maxscore(CursorContainer cursors,
                   Payload init,
                   AccumulateFn accumulate,
                   ThresholdFn threshold)
{
    return MaxScoreJoin<CursorContainer, Payload, AccumulateFn, ThresholdFn>(
        std::move(cursors), std::move(init), std::move(accumulate), std::move(threshold));
}

template <typename Index, typename Scorer>
auto maxscore(Query const& query,
              Index const& index,
              topk_queue topk,
              Scorer&& scorer,
              decltype(index.max_scored_cursor(0, scorer).max_score()) initial_threshold)
{
    using cursor_type = decltype(index.max_scored_cursor(0, scorer));
    using score_type = decltype(index.max_scored_cursor(0, scorer).max_score());
    using value_type = decltype(index.max_scored_cursor(0, scorer).value());

    std::vector<cursor_type> cursors;
    std::transform(query.terms.begin(),
                   query.terms.end(),
                   std::back_inserter(cursors),
                   [&](auto term) { return index.max_scored_cursor(term, scorer); });

    auto joined = join_maxscore(
        std::move(cursors),
        0.0F,
        [](auto& score, auto& cursor, auto /* term_position */) {
            score += cursor.payload();
            return score;
        },
        [&](auto score) { return topk.would_enter(score) && score > initial_threshold; });
    v1::for_each(joined, [&](auto& cursor) { topk.insert(cursor.payload(), cursor.value()); });
    return topk;
}

template <typename Index, typename Scorer>
auto maxscore(Query const& query, Index const& index, topk_queue topk, Scorer&& scorer)
{
    using cursor_type = decltype(index.max_scored_cursor(0, scorer));
    using score_type = decltype(index.max_scored_cursor(0, scorer).max_score());
    using value_type = decltype(index.max_scored_cursor(0, scorer).value());

    std::vector<cursor_type> cursors;
    std::transform(query.terms.begin(),
                   query.terms.end(),
                   std::back_inserter(cursors),
                   [&](auto term) { return index.max_scored_cursor(term, scorer); });

    auto joined = join_maxscore(
        std::move(cursors),
        0.0F,
        [](auto& score, auto& cursor, auto /* term_position */) {
            score += cursor.payload();
            return score;
        },
        [&](auto score) { return topk.would_enter(score); });
    v1::for_each(joined, [&](auto& cursor) { topk.insert(cursor.payload(), cursor.value()); });
    return topk;
    // template <typename CursorContainer, typename Payload, typename AccumulateFn, typename
    // ThresholdFn> MaxScoreJoin {

    // std::vector<cursor_type*> sorted_cursors;
    // std::transform(cursors.begin(),
    //               cursors.end(),
    //               std::back_inserter(sorted_cursors),
    //               [](auto&& cursor) { return &cursor; });
    // std::sort(sorted_cursors.begin(), sorted_cursors.end(), [](auto&& lhs, auto&& rhs) {
    //    return lhs->max_score() < rhs->max_score();
    //});

    // std::vector<score_type> upper_bounds(sorted_cursors.size());
    //// upper_bounds.push_back(cursors);
    //// for (auto* cursor : sorted_cursors) {
    ////
    ////}
    // upper_bounds[0] = sorted_cursors[0]->max_score();
    // for (size_t i = 1; i < sorted_cursors.size(); ++i) {
    //    upper_bounds[i] = upper_bounds[i - 1] + sorted_cursors[i]->max_score();
    //}
    //// std::partial_sum(sorted_cursors.begin(),
    ////                 sorted_cursors.end(),
    ////                 upper_bounds.begin(),
    ////                 [](auto sum, auto* cursor) { return sum + cursor->max_score(); });
    // auto essential = gsl::make_span(sorted_cursors);
    // auto non_essential = essential.subspan(essential.size()); // Empty

    // DocId current_docid =
    //    std::min_element(cursors.begin(), cursors.end(), [](auto&& lhs, auto&& rhs) {
    //        return *lhs < *rhs;
    //    })->value();

    // while (not essential.empty() && current_docid < std::numeric_limits<value_type>::max()) {
    //    score_type score{};
    //    auto next_docid = std::numeric_limits<value_type>::max();

    //    for (auto cursor : essential) {
    //        if (cursor->value() == current_docid) {
    //            score += cursor->payload();
    //            cursor->advance();
    //        }
    //        if (auto docid = cursor->value(); docid < next_docid) {
    //            next_docid = docid;
    //        }
    //    }

    //    for (auto idx = non_essential.size() - 1; idx + 1 > 0; idx -= 1) {
    //        if (not topk.would_enter(score + upper_bounds[idx])) {
    //            break;
    //        }
    //        sorted_cursors[idx]->advance_to_geq(current_docid);
    //        if (sorted_cursors[idx]->value() == current_docid) {
    //            score += sorted_cursors[idx]->payload();
    //        }
    //    }
    //    if (topk.insert(score, current_docid)) {
    //        //// update non-essential lists
    //        while (not essential.empty()
    //               && not topk.would_enter(upper_bounds[non_essential.size()])) {
    //            essential = essential.first(essential.size() - 1);
    //            non_essential = gsl::make_span(sorted_cursors).subspan(essential.size());
    //        }
    //    }
    //    current_docid = next_docid;
    //}

    // return topk;
}

} // namespace pisa::v1
