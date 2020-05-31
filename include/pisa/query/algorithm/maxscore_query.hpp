#pragma once

#include <deque>
#include <numeric>
#include <vector>

#include <gsl/span>
#include <range/v3/algorithm/reverse.hpp>
#include <range/v3/algorithm/sort.hpp>
#include <range/v3/view/reverse.hpp>

#include "cursor/cursor.hpp"
#include "cursor/cursor_union.hpp"
#include "query/queries.hpp"
#include "topk_queue.hpp"

namespace pisa {

template <typename Cursors, typename Payload, typename AccumulateFn, typename ThresholdFn>
struct MaxscoreJoin {
    using cursor_type = std::decay_t<typename Cursors::value_type>;
    using payload_type = Payload;
    using value_type = std::uint32_t;

    MaxscoreJoin(
        Cursors cursors,
        Payload init,
        AccumulateFn accumulate,
        ThresholdFn above_threshold,
        std::uint32_t sentinel)
        : m_cursors(std::move(cursors)),
          m_first_lookup(m_cursors.end()),
          m_init(std::move(init)),
          m_accumulate(std::move(accumulate)),
          m_above_threshold(std::move(above_threshold))
    {
        m_upper_bounds.resize(m_cursors.size());
        pisa::inclusive_scan(
            m_cursors.rbegin(),
            m_cursors.rend(),
            m_upper_bounds.rbegin(),
            [](auto acc, auto&& cursor) { return acc + cursor.max_score(); },
            0.0);
        m_first_upper_bound = m_upper_bounds.end();
        // TODO(michal): automatic sentinel inference.
        // m_sentinel = essential_cursor.sentinel();
        m_sentinel = sentinel;
        m_next_docid =
            std::min_element(m_cursors.begin(), m_first_lookup, [](auto const& lhs, auto const& rhs) {
                return lhs.docid() < rhs.docid();
            })->docid();
        while (m_first_lookup != m_cursors.begin()
               && !m_above_threshold(*std::prev(m_first_upper_bound))) {
            std::advance(m_first_lookup, -1);
            std::advance(m_first_upper_bound, -1);
            if (m_first_lookup == m_cursors.begin()) {
                m_current_value = m_sentinel;
                return;
            }
        }
        next();
    }

    [[nodiscard]] constexpr PISA_ALWAYSINLINE auto docid() const noexcept -> value_type
    {
        return m_current_value;
    }
    [[nodiscard]] constexpr PISA_ALWAYSINLINE auto score() const noexcept -> Payload const&
    {
        return payload();
    }
    [[nodiscard]] constexpr PISA_ALWAYSINLINE auto payload() const noexcept -> Payload const&
    {
        return m_current_payload;
    }
    [[nodiscard]] constexpr PISA_ALWAYSINLINE auto sentinel() const noexcept -> std::uint32_t
    {
        return m_sentinel;
    }

    constexpr PISA_ALWAYSINLINE void next()
    {
        bool exit = false;
        while (not exit) {
            if (PISA_UNLIKELY(m_next_docid == this->sentinel())) {
                m_current_value = m_sentinel;
                return;
            }

            m_current_payload = m_init;
            m_current_value = m_next_docid;
            m_next_docid = m_sentinel;
            for (auto iter = m_cursors.begin(); iter != m_first_lookup; std::advance(iter, 1)) {
                auto& cursor = *iter;
                if (cursor.docid() == m_current_value) {
                    m_current_payload = m_accumulate(m_current_payload, cursor);
                    cursor.next();
                }
                if (auto value = cursor.docid(); value < m_next_docid) {
                    m_next_docid = value;
                }
            }

            exit = true;
            auto lookup_bound = m_first_upper_bound;
            for (auto iter = m_first_lookup; iter != m_cursors.end(); std::advance(iter, 1)) {
                auto& cursor = *iter;
                if (not m_above_threshold(m_current_payload + *lookup_bound)) {
                    exit = false;
                    break;
                }
                cursor.next_geq(m_current_value);
                if (cursor.docid() == m_current_value) {
                    m_current_payload = m_accumulate(m_current_payload, cursor);
                }
                std::advance(lookup_bound, 1);
            }
        }
        if (m_above_threshold(m_current_payload)) {
            while (m_first_lookup != m_cursors.begin()
                   && !m_above_threshold(*std::prev(m_first_upper_bound))) {
                std::advance(m_first_lookup, -1);
                std::advance(m_first_upper_bound, -1);
                if (m_first_lookup == m_cursors.begin()) {
                    m_current_value = m_sentinel;
                    return;
                }
            }
        }
    }

    [[nodiscard]] constexpr PISA_ALWAYSINLINE auto empty() const noexcept -> bool
    {
        return m_current_value >= sentinel();
    }

  private:
    Cursors m_cursors;
    typename Cursors::iterator m_first_lookup;
    payload_type m_init;
    AccumulateFn m_accumulate;
    ThresholdFn m_above_threshold;

    value_type m_current_value{};
    value_type m_sentinel{};
    value_type m_next_docid{};
    payload_type m_current_payload{};
    std::vector<payload_type> m_upper_bounds{};
    typename std::vector<payload_type>::const_iterator m_first_upper_bound;
};

template <typename Cursors, typename Payload, typename AccumulateFn, typename ThresholdFn>
auto join_maxscore(
    Cursors cursors,
    Payload init,
    AccumulateFn accumulate,
    ThresholdFn above_threshold,
    std::uint32_t sentinel)
{
    using cursor_type = std::decay_t<typename Cursors::value_type>;
    std::vector<std::size_t> term_positions(cursors.size());
    std::iota(term_positions.begin(), term_positions.end(), 0);
    ranges::sort(
        term_positions, std::greater{}, [&](auto&& pos) { return cursors[pos].max_score(); });
    std::vector<cursor_type> ordered_cursors;
    for (auto pos: term_positions) {
        ordered_cursors.push_back(std::move(cursors[pos]));
    };

    return MaxscoreJoin<std::vector<cursor_type>, Payload, AccumulateFn, ThresholdFn>(
        std::move(ordered_cursors),
        std::move(init),
        std::move(accumulate),
        std::move(above_threshold),
        sentinel);
}

struct maxscore_query {
    explicit maxscore_query(topk_queue& topk) : m_topk(topk) {}

    template <typename Cursors>
    [[nodiscard]] PISA_ALWAYSINLINE auto sorted(Cursors&& cursors)
        -> std::vector<typename std::decay_t<Cursors>::value_type>
    {
        std::vector<std::size_t> term_positions(cursors.size());
        std::iota(term_positions.begin(), term_positions.end(), 0);
        ranges::sort(
            term_positions, std::greater{}, [&](auto&& pos) { return cursors[pos].max_score(); });
        std::vector<typename std::decay_t<Cursors>::value_type> sorted;
        for (auto pos: term_positions) {
            sorted.push_back(std::move(cursors[pos]));
        };
        return sorted;
    }

    template <typename Cursors>
    [[nodiscard]] PISA_ALWAYSINLINE auto calc_upper_bounds(Cursors&& cursors) -> std::vector<float>
    {
        std::vector<float> upper_bounds(cursors.size());
        auto out = upper_bounds.rbegin();
        float bound = 0.0;
        for (auto pos = cursors.rbegin(); pos != cursors.rend(); ++pos) {
            bound += pos->max_score();
            *out++ = bound;
        }
        return upper_bounds;
    }

    template <typename Cursors>
    [[nodiscard]] PISA_ALWAYSINLINE auto min_docid(Cursors&& cursors) -> std::uint32_t
    {
        return std::min_element(
                   cursors.begin(),
                   cursors.end(),
                   [](auto&& lhs, auto&& rhs) { return lhs.docid() < rhs.docid(); })
            ->docid();
    }

    enum class UpdateResult : bool { Continue, ShortCircuit };
    enum class DocumentStatus : bool { Insert, Skip };

    template <typename Cursors>
    void operator()(Cursors&& cursors_, uint64_t max_docid)
    {
        using cursor_type = typename std::decay_t<Cursors>::value_type;
        if (cursors_.empty()) {
            return;
        }
        auto cursors = sorted(cursors_);

        auto upper_bounds = calc_upper_bounds(cursors);
        auto above_threshold = [&](auto score) { return m_topk.would_enter(score); };

        auto first_upper_bound = upper_bounds.end();
        auto first_lookup = cursors.end();
        auto next_docid = min_docid(cursors);

        auto update_non_essential_lists = [&] {
            while (first_lookup != cursors.begin()
                   && !above_threshold(*std::prev(first_upper_bound))) {
                --first_lookup;
                --first_upper_bound;
                if (first_lookup == cursors.begin()) {
                    return UpdateResult::ShortCircuit;
                }
            }
            return UpdateResult::Continue;
        };

        if (update_non_essential_lists() == UpdateResult::ShortCircuit) {
            std::swap(cursors, cursors_);
            return;
        }

        float current_score = 0;
        std::uint32_t current_docid = 0;

        while (current_docid < max_docid) {
            auto status = DocumentStatus::Skip;
            while (status == DocumentStatus::Skip) {
                if (PISA_UNLIKELY(next_docid >= max_docid)) {
                    std::swap(cursors, cursors_);
                    return;
                }

                current_score = 0;
                current_docid = std::exchange(next_docid, max_docid);

                std::for_each(cursors.begin(), first_lookup, [&](auto& cursor) {
                    if (cursor.docid() == current_docid) {
                        current_score += cursor.score();
                        cursor.next();
                    }
                    if (auto docid = cursor.docid(); docid < next_docid) {
                        next_docid = docid;
                    }
                });

                status = DocumentStatus::Insert;
                auto lookup_bound = first_upper_bound;
                for (auto pos = first_lookup; pos != cursors.end(); ++pos, ++lookup_bound) {
                    auto& cursor = *pos;
                    if (not above_threshold(current_score + *lookup_bound)) {
                        status = DocumentStatus::Skip;
                        break;
                    }
                    cursor.next_geq(current_docid);
                    if (cursor.docid() == current_docid) {
                        current_score += cursor.score();
                    }
                }
            }
            if (m_topk.insert(current_score, current_docid)
                && update_non_essential_lists() == UpdateResult::ShortCircuit) {
                std::swap(cursors, cursors_);
                return;
            }
        }
        std::swap(cursors, cursors_);
    }

    std::vector<std::pair<float, uint64_t>> const& topk() const { return m_topk.topk(); }

  private:
    topk_queue& m_topk;
};

}  // namespace pisa
