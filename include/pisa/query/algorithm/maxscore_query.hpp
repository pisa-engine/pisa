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

    // template <typename CursorRange>
    // void operator()(CursorRange&& cursors, uint64_t max_docid)
    //{
    //    using Cursor = typename std::decay_t<CursorRange>::value_type;
    //    if (cursors.empty()) {
    //        return;
    //    }

    //    std::vector<Cursor*> ordered_cursors;
    //    ordered_cursors.reserve(cursors.size());
    //    for (auto& en: cursors) {
    //        ordered_cursors.push_back(&en);
    //    }

    //    // sort enumerators by increasing maxscore
    //    std::sort(ordered_cursors.begin(), ordered_cursors.end(), [](Cursor* lhs, Cursor* rhs) {
    //        return lhs->max_score() < rhs->max_score();
    //    });

    //    std::vector<float> upper_bounds(ordered_cursors.size());
    //    upper_bounds[0] = ordered_cursors[0]->max_score();
    //    for (size_t i = 1; i < ordered_cursors.size(); ++i) {
    //        upper_bounds[i] = upper_bounds[i - 1] + ordered_cursors[i]->max_score();
    //    }

    //    uint64_t non_essential_lists = 0;
    //    auto update_non_essential_lists = [&]() {
    //        while (non_essential_lists < ordered_cursors.size()
    //               && !m_topk.would_enter(upper_bounds[non_essential_lists])) {
    //            non_essential_lists += 1;
    //        }
    //    };
    //    update_non_essential_lists();

    //    uint64_t cur_doc =
    //        std::min_element(cursors.begin(), cursors.end(), [](Cursor const& lhs, Cursor const&
    //        rhs) {
    //            return lhs.docid() < rhs.docid();
    //        })->docid();

    //    while (non_essential_lists < ordered_cursors.size() && cur_doc < max_docid) {
    //        float score = 0;
    //        uint64_t next_doc = max_docid;
    //        for (size_t i = non_essential_lists; i < ordered_cursors.size(); ++i) {
    //            if (ordered_cursors[i]->docid() == cur_doc) {
    //                score += ordered_cursors[i]->score();
    //                ordered_cursors[i]->next();
    //            }
    //            if (ordered_cursors[i]->docid() < next_doc) {
    //                next_doc = ordered_cursors[i]->docid();
    //            }
    //        }

    //        // try to complete evaluation with non-essential lists
    //        for (size_t i = non_essential_lists - 1; i + 1 > 0; --i) {
    //            if (!m_topk.would_enter(score + upper_bounds[i])) {
    //                break;
    //            }
    //            ordered_cursors[i]->next_geq(cur_doc);
    //            if (ordered_cursors[i]->docid() == cur_doc) {
    //                score += ordered_cursors[i]->score();
    //            }
    //        }

    //        if (m_topk.insert(score, cur_doc)) {
    //            update_non_essential_lists();
    //        }

    //        cur_doc = next_doc;
    //    }
    //}

    // template <typename CursorRange>
    // void operator()(CursorRange&& cursors, uint64_t max_docid)
    //{
    //    using cursor_type = typename std::decay_t<CursorRange>::value_type;
    //    if (cursors.empty()) {
    //        return;
    //    }

    //    std::vector<std::size_t> term_positions(cursors.size());
    //    std::iota(term_positions.begin(), term_positions.end(), 0);
    //    ranges::sort(
    //        term_positions, std::less{}, [&](auto&& pos) { return cursors[pos].max_score(); });
    //    std::vector<cursor_type> ordered_cursors;
    //    for (auto pos: term_positions) {
    //        ordered_cursors.push_back(std::move(cursors[pos]));
    //    };
    //    auto above_threshold = [&](auto score) { return m_topk.would_enter(score); };

    //    auto next_docid =
    //        std::min_element(
    //            ordered_cursors.begin(),
    //            ordered_cursors.end(),
    //            [](auto const& lhs, auto const& rhs) { return lhs.docid() < rhs.docid(); })
    //            ->docid();

    //    std::vector<float> upper_bounds(ordered_cursors.size());
    //    upper_bounds[0] = ordered_cursors[0].max_score();
    //    for (size_t i = 1; i < ordered_cursors.size(); ++i) {
    //        upper_bounds[i] = upper_bounds[i - 1] + ordered_cursors[i].max_score();
    //    }

    //    uint64_t non_essential_lists = 0;
    //    auto update_non_essential_lists = [&]() {
    //        while (non_essential_lists < ordered_cursors.size()
    //               && !m_topk.would_enter(upper_bounds[non_essential_lists])) {
    //            non_essential_lists += 1;
    //        }
    //    };
    //    update_non_essential_lists();
    //    if (non_essential_lists == ordered_cursors.size()) {
    //        return;
    //    }

    //    float score = 0;
    //    auto current_docid = next_docid;

    //    while (current_docid < max_docid) {
    //        bool exit = false;
    //        while (not exit) {
    //            if (PISA_UNLIKELY(next_docid >= max_docid)) {
    //                return;
    //            }
    //            score = 0;
    //            current_docid = next_docid;
    //            next_docid = max_docid;
    //            // for (auto iter = ordered_cursors.rbegin();
    //            //     iter != std::prev(ordered_cursors.rend(), non_essential_lists);
    //            //     std::advance(iter, 1)) {
    //            for (auto iter = std::next(ordered_cursors.begin(), non_essential_lists);
    //                 iter != ordered_cursors.end();
    //                 std::advance(iter, 1)) {
    //                auto& cursor = *iter;
    //                // for (auto pos = non_essential_lists; pos < ordered_cursors.size(); pos +=1)
    //                // {
    //                // auto& cursor = ordered_cursors[pos];
    //                if (cursor.docid() == current_docid) {
    //                    score += cursor.score();
    //                    cursor.next();
    //                }
    //                if (cursor.docid() < next_docid) {
    //                    next_docid = cursor.docid();
    //                }
    //            }
    //            exit = true;
    //            // auto lookup_bound = first_upper_bound;
    //            // for (auto iter = first_lookup; iter != ordered_cursors.rend();
    //            std::advance(iter,
    //            // 1)) {
    //            auto lookup_bound = std::prev(upper_bounds.rend(), non_essential_lists);
    //            for (auto iter = std::prev(ordered_cursors.rend(), non_essential_lists);
    //                 iter != ordered_cursors.rend();
    //                 std::advance(iter, 1)) {
    //                auto& cursor = *iter;
    //                if (not above_threshold(score + *lookup_bound)) {
    //                    exit = false;
    //                    break;
    //                }
    //                cursor.next_geq(current_docid);
    //                if (cursor.docid() == current_docid) {
    //                    score += cursor.score();
    //                }
    //                std::advance(lookup_bound, 1);
    //            }
    //        }
    //        if (m_topk.insert(score, current_docid)) {
    //            update_non_essential_lists();
    //            // if (first_lookup == ordered_cursors.rbegin()) {
    //            if (non_essential_lists == ordered_cursors.size()) {
    //                return;
    //            }
    //        }
    //    }
    //}

    // template <typename CursorRange>
    // void operator()(CursorRange&& cursors, uint64_t max_docid)
    //{
    //    using cursor_type = typename std::decay_t<CursorRange>::value_type;
    //    if (cursors.empty()) {
    //        return;
    //    }

    //    auto joined = join_maxscore(
    //        std::move(cursors),
    //        0.0,
    //        Add{},
    //        [&](auto score) { return m_topk.would_enter(score); },
    //        max_docid);
    //    while (not joined.empty()) {
    //        m_topk.insert(joined.score(), joined.docid());
    //        joined.next();
    //    }
    //}

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
        pisa::inclusive_scan(
            cursors.rbegin(),
            cursors.rend(),
            upper_bounds.rbegin(),
            [](auto acc, auto&& cursor) { return acc + cursor.max_score(); },
            0.0);
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

    // template <typename CursorIterator>
    //[[nodiscard]] auto accumulate_essential(CursorIterator first, CursorIterator last) -> int
    //{
    //    auto next_docid = max_docid;
    //    for (auto iter = cursors.begin(); iter != first_lookup; std::advance(iter, 1)) {
    //        auto& cursor = *iter;
    //        if (cursor.docid() == current_docid) {
    //            current_score += cursor.score();
    //            cursor.next();
    //        }
    //        if (auto docid = cursor.docid(); docid < next_docid) {
    //            next_docid = docid;
    //        }
    //    }
    //    return next_docid;
    //}

    template <typename Cursors>
    void operator()(Cursors&& unordered_cursors, uint64_t max_docid)
    {
        using cursor_type = typename std::decay_t<Cursors>::value_type;
        if (unordered_cursors.empty()) {
            return;
        }
        auto cursors = sorted(unordered_cursors);
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
            return;
        }

        float current_score = 0;
        std::uint32_t current_docid = 0;

        while (current_docid < max_docid) {
            auto status = DocumentStatus::Skip;
            while (status == DocumentStatus::Skip) {
                if (PISA_UNLIKELY(next_docid >= max_docid)) {
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
                return;
            }
        }
    }

    std::vector<std::pair<float, uint64_t>> const& topk() const { return m_topk.topk(); }

  private:
    topk_queue& m_topk;
};

}  // namespace pisa
