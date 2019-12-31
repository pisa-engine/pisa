#pragma once

#include <algorithm>
#include <numeric>
#include <vector>

#include <gsl/span>
#include <range/v3/view/iota.hpp>

#include "util/compiler_attribute.hpp"
#include "v1/algorithm.hpp"
#include "v1/cursor/scoring_cursor.hpp"
#include "v1/cursor_accumulator.hpp"
#include "v1/query.hpp"

namespace pisa::v1 {

template <typename CursorContainer,
          typename Payload,
          typename AccumulateFn,
          typename ThresholdFn,
          typename Inspect>
struct BlockMaxWandJoin;

template <typename CursorContainer,
          typename Payload,
          typename AccumulateFn,
          typename ThresholdFn,
          typename Inspect = void>
struct WandJoin {
    using cursor_type = typename CursorContainer::value_type;
    using payload_type = Payload;
    using value_type = std::decay_t<decltype(*std::declval<cursor_type>())>;

    friend BlockMaxWandJoin<CursorContainer, Payload, AccumulateFn, ThresholdFn, Inspect>;

    using iterator_category =
        typename std::iterator_traits<typename CursorContainer::iterator>::iterator_category;
    static_assert(std::is_base_of<std::random_access_iterator_tag, iterator_category>(),
                  "cursors must be stored in a random access container");

    constexpr WandJoin(CursorContainer cursors,
                       Payload init,
                       AccumulateFn accumulate,
                       ThresholdFn above_threshold)
        : m_cursors(std::move(cursors)),
          m_cursor_pointers(m_cursors.size()),
          m_init(std::move(init)),
          m_accumulate(std::move(accumulate)),
          m_above_threshold(std::move(above_threshold)),
          m_size(std::nullopt)
    {
        initialize();
    }

    constexpr WandJoin(CursorContainer cursors,
                       Payload init,
                       AccumulateFn accumulate,
                       ThresholdFn above_threshold,
                       Inspect* inspect)
        : m_cursors(std::move(cursors)),
          m_cursor_pointers(m_cursors.size()),
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
        std::transform(m_cursors.begin(),
                       m_cursors.end(),
                       m_cursor_pointers.begin(),
                       [](auto&& cursor) { return &cursor; });

        std::sort(m_cursor_pointers.begin(), m_cursor_pointers.end(), [](auto&& lhs, auto&& rhs) {
            return lhs->value() < rhs->value();
        });

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

    PISA_ALWAYSINLINE void advance()
    {
        bool exit = false;
        while (not exit) {
            auto upper_bound = 0.0F;
            std::size_t pivot;
            bool found_pivot = false;
            for (pivot = 0; pivot < m_cursor_pointers.size(); ++pivot) {
                if (m_cursor_pointers[pivot]->empty()) {
                    break;
                }
                upper_bound += m_cursor_pointers[pivot]->max_score();
                if (m_above_threshold(upper_bound)) {
                    found_pivot = true;
                    break;
                }
            }
            // auto pivot = find_pivot();
            // if (PISA_UNLIKELY(not pivot)) {
            //    m_current_value = sentinel();
            //    exit = true;
            //    break;
            //}
            if (not found_pivot) {
                m_current_value = sentinel();
                exit = true;
                break;
            }

            // auto pivot_docid = (*pivot)->value();
            auto pivot_docid = m_cursor_pointers[pivot]->value();
            if (pivot_docid == m_cursor_pointers.front()->value()) {
                m_current_value = pivot_docid;
                m_current_payload = m_init;

                for (auto* cursor : m_cursor_pointers) {
                    if (cursor->value() != pivot_docid) {
                        break;
                    }
                    m_current_payload = m_accumulate(m_current_payload, *cursor);
                    cursor->advance();
                }

                auto by_docid = [](auto&& lhs, auto&& rhs) { return lhs->value() < rhs->value(); };
                std::sort(m_cursor_pointers.begin(), m_cursor_pointers.end(), by_docid);
                exit = true;
            } else {
                auto next_list = pivot;
                for (; m_cursor_pointers[next_list]->value() == pivot_docid; --next_list) {
                }
                m_cursor_pointers[next_list]->advance_to_geq(pivot_docid);
                // bubble_down(next_list);
                for (size_t idx = next_list + 1; idx < m_cursor_pointers.size(); idx += 1) {
                    if (m_cursor_pointers[idx]->value() < m_cursor_pointers[idx - 1]->value()) {
                        std::swap(m_cursor_pointers[idx], m_cursor_pointers[idx - 1]);
                    } else {
                        break;
                    }
                }
            }
        }
    }

    [[nodiscard]] constexpr auto empty() const noexcept -> bool
    {
        return m_current_value >= sentinel();
    }

   private:
    PISA_ALWAYSINLINE void bubble_down(std::size_t list_idx)
    {
        for (size_t idx = list_idx + 1; idx < m_cursor_pointers.size(); idx += 1) {
            if (m_cursor_pointers[idx]->value() < m_cursor_pointers[idx - 1]->value()) {
                std::swap(m_cursor_pointers[idx], m_cursor_pointers[idx - 1]);
            } else {
                break;
            }
        }
    }

    PISA_ALWAYSINLINE auto find_pivot() -> tl::optional<std::size_t>
    {
        auto upper_bound = 0.0F;
        std::size_t pivot;
        for (pivot = 0; pivot < m_cursor_pointers.size(); ++pivot) {
            if (m_cursor_pointers[pivot]->empty()) {
                break;
            }
            upper_bound += m_cursor_pointers[pivot]->max_score();
            if (m_above_threshold(upper_bound)) {
                return tl::make_optional(pivot);
            }
        }
        return tl::nullopt;
        // auto upper_bound = 0.0F;
        // for (auto pivot = m_cursor_pointers.begin(); pivot != m_cursor_pointers.end(); ++pivot) {
        //    auto&& cursor = **pivot;
        //    if (cursor.empty()) {
        //        break;
        //    }
        //    upper_bound += cursor.max_score();
        //    if (m_above_threshold(upper_bound)) {
        //        auto pivot_docid = (*pivot)->value();
        //        while (std::next(pivot) != m_cursor_pointers.end()) {
        //            if ((*std::next(pivot))->value() != pivot_docid) {
        //                break;
        //            }
        //            pivot = std::next(pivot);
        //        }
        //        return pivot;
        //    }
        //}
        // return m_cursor_pointers.end();
    }

    CursorContainer m_cursors;
    std::vector<cursor_type*> m_cursor_pointers;
    payload_type m_init;
    AccumulateFn m_accumulate;
    ThresholdFn m_above_threshold;
    std::optional<std::size_t> m_size;

    value_type m_current_value{};
    value_type m_sentinel{};
    payload_type m_current_payload{};
    payload_type m_previous_threshold{};

    Inspect* m_inspect;
};

template <typename CursorContainer,
          typename Payload,
          typename AccumulateFn,
          typename ThresholdFn,
          typename Inspect = void>
struct BlockMaxWandJoin {
    using cursor_type = typename CursorContainer::value_type;
    using payload_type = Payload;
    using value_type = std::decay_t<decltype(*std::declval<cursor_type>())>;

    using iterator_category =
        typename std::iterator_traits<typename CursorContainer::iterator>::iterator_category;
    static_assert(std::is_base_of<std::random_access_iterator_tag, iterator_category>(),
                  "cursors must be stored in a random access container");

    constexpr BlockMaxWandJoin(CursorContainer cursors,
                               Payload init,
                               AccumulateFn accumulate,
                               ThresholdFn above_threshold)
        : m_wand_join(std::move(cursors), init, std::move(accumulate), std::move(above_threshold))
    {
    }

    constexpr BlockMaxWandJoin(CursorContainer cursors,
                               Payload init,
                               AccumulateFn accumulate,
                               ThresholdFn above_threshold,
                               Inspect* inspect)
        : WandJoin<CursorContainer, Payload, AccumulateFn, ThresholdFn, Inspect>(
            std::move(cursors), init, std::move(accumulate), std::move(above_threshold), inspect)
    {
    }

    [[nodiscard]] constexpr auto operator*() const noexcept -> value_type
    {
        return m_wand_join.value();
    }
    [[nodiscard]] constexpr auto value() const noexcept -> value_type
    {
        return m_wand_join.value();
    }
    [[nodiscard]] constexpr auto payload() const noexcept -> Payload const&
    {
        return m_wand_join.payload();
    }
    [[nodiscard]] constexpr auto sentinel() const noexcept -> std::uint32_t
    {
        return m_wand_join.sentinel();
    }
    [[nodiscard]] constexpr auto empty() const noexcept -> bool { return m_wand_join.empty(); }

    constexpr void advance() { m_wand_join.advance(); }
    // constexpr void advance()
    //{
    //    while (true) {
    //        auto pivot = m_wand_join.find_pivot();
    //        if (pivot == m_wand_join.m_cursor_pointers.end()) {
    //            m_wand_join.m_current_value = m_wand_join.sentinel();
    //            return;
    //        }

    //        auto pivot_docid = (*pivot)->value();
    //        // auto block_upper_bound = std::accumulate(
    //        //    m_wand_join.m_cursor_pointers.begin(),
    //        //    std::next(pivot),
    //        //    0.0F,
    //        //    [&](auto acc, auto* cursor) { return acc + cursor->block_max_score(pivot_docid);
    //        //    });
    //        // if (not m_wand_join.m_above_threshold(block_upper_bound)) {
    //        //    block_max_advance(pivot, pivot_docid);
    //        //    continue;
    //        //}
    //        if (pivot_docid == m_wand_join.m_cursor_pointers.front()->value()) {
    //            m_wand_join.m_current_value = pivot_docid;
    //            m_wand_join.m_current_payload = m_wand_join.m_init;

    //            [&]() {
    //                auto iter = m_wand_join.m_cursor_pointers.begin();
    //                for (; iter != m_wand_join.m_cursor_pointers.end(); ++iter) {
    //                    auto* cursor = *iter;
    //                    if (cursor->value() != pivot_docid) {
    //                        break;
    //                    }
    //                    m_wand_join.m_current_payload =
    //                        m_wand_join.m_accumulate(m_wand_join.m_current_payload, *cursor);
    //                    cursor->advance();
    //                }
    //                return iter;
    //            }();
    //            // for (auto* cursor : m_wand_join.m_cursor_pointers) {
    //            //    if (cursor->value() != pivot_docid) {
    //            //        break;
    //            //    }
    //            //    m_wand_join.m_current_payload =
    //            //        m_wand_join.m_accumulate(m_wand_join.m_current_payload, *cursor);
    //            //    block_upper_bound -= cursor->block_max_score() - cursor->payload();
    //            //    if (not m_wand_join.m_above_threshold(block_upper_bound)) {
    //            //        break;
    //            //    }
    //            //}

    //            // for (auto* cursor : m_wand_join.m_cursor_pointers) {
    //            //    if (cursor->value() != pivot_docid) {
    //            //        break;
    //            //    }
    //            //    cursor->advance();
    //            //}

    //            auto by_docid = [](auto&& lhs, auto&& rhs) { return lhs->value() < rhs->value();
    //            }; std::sort(m_wand_join.m_cursor_pointers.begin(),
    //                      m_wand_join.m_cursor_pointers.end(),
    //                      by_docid);
    //            return;
    //        }

    //        auto next_list = std::distance(m_wand_join.m_cursor_pointers.begin(), pivot);
    //        for (; m_wand_join.m_cursor_pointers[next_list]->value() == pivot_docid; --next_list)
    //        {
    //        }
    //        m_wand_join.m_cursor_pointers[next_list]->advance_to_geq(pivot_docid);
    //        m_wand_join.bubble_down(next_list);
    //    }
    //}

   private:
    template <typename Iter>
    void block_max_advance(Iter pivot, DocId pivot_id)
    {
        auto next_list = std::max_element(
            m_wand_join.m_cursor_pointers.begin(), std::next(pivot), [](auto* lhs, auto* rhs) {
                return lhs->max_score() < rhs->max_score();
            });

        auto next_docid =
            (*std::min_element(m_wand_join.m_cursor_pointers.begin(),
                               std::next(pivot),
                               [](auto* lhs, auto* rhs) {
                                   return lhs->block_max_docid() < rhs->block_max_docid();
                               }))
                ->value();
        next_docid += 1;

        if (auto iter = std::next(pivot); iter != m_wand_join.m_cursor_pointers.end()) {
            if (auto docid = (*iter)->value(); docid < next_docid) {
                next_docid = docid;
            }
        }

        if (next_docid <= pivot_id) {
            next_docid = pivot_id + 1;
        }

        (*next_list)->advance_to_geq(next_docid);
        m_wand_join.bubble_down(std::distance(m_wand_join.m_cursor_pointers.begin(), next_list));
    }

    WandJoin<CursorContainer, Payload, AccumulateFn, ThresholdFn, Inspect> m_wand_join;
};

template <typename CursorContainer, typename Payload, typename AccumulateFn, typename ThresholdFn>
auto join_wand(CursorContainer cursors,
               Payload init,
               AccumulateFn accumulate,
               ThresholdFn threshold)
{
    return WandJoin<CursorContainer, Payload, AccumulateFn, ThresholdFn, void>(
        std::move(cursors), std::move(init), std::move(accumulate), std::move(threshold));
}

template <typename CursorContainer,
          typename Payload,
          typename AccumulateFn,
          typename ThresholdFn,
          typename Inspect>
auto join_wand(CursorContainer cursors,
               Payload init,
               AccumulateFn accumulate,
               ThresholdFn threshold,
               Inspect* inspect)
{
    return WandJoin<CursorContainer, Payload, AccumulateFn, ThresholdFn, Inspect>(
        std::move(cursors), std::move(init), std::move(accumulate), std::move(threshold), inspect);
}

template <typename CursorContainer, typename Payload, typename AccumulateFn, typename ThresholdFn>
auto join_block_max_wand(CursorContainer cursors,
                         Payload init,
                         AccumulateFn accumulate,
                         ThresholdFn threshold)
{
    return BlockMaxWandJoin<CursorContainer, Payload, AccumulateFn, ThresholdFn, void>(
        std::move(cursors), std::move(init), std::move(accumulate), std::move(threshold));
}

template <typename CursorContainer,
          typename Payload,
          typename AccumulateFn,
          typename ThresholdFn,
          typename AdvanceFn,
          typename Inspect>
auto join_block_max_wand(CursorContainer cursors,
                         Payload init,
                         AccumulateFn accumulate,
                         ThresholdFn threshold,
                         Inspect* inspect)
{
    return BlockMaxWandJoin<CursorContainer, Payload, AccumulateFn, ThresholdFn, Inspect>(
        std::move(cursors), std::move(init), std::move(accumulate), std::move(threshold), inspect);
}

template <typename Index, typename Scorer>
auto wand(Query const& query, Index const& index, topk_queue topk, Scorer&& scorer)
{
    auto const& term_ids = query.get_term_ids();
    if (term_ids.empty()) {
        return topk;
    }
    auto cursors = index.max_scored_cursors(gsl::make_span(term_ids), scorer);
    if (query.threshold()) {
        topk.set_threshold(*query.threshold());
    }
    auto joined = join_wand(std::move(cursors), 0.0F, accumulate::Add{}, [&](auto score) {
        return topk.would_enter(score);
    });
    v1::for_each(joined, [&](auto& cursor) { topk.insert(cursor.payload(), cursor.value()); });
    return topk;
}

template <typename Index, typename Scorer>
auto bmw(Query const& query, Index const& index, topk_queue topk, Scorer&& scorer)
{
    auto const& term_ids = query.get_term_ids();
    if (term_ids.empty()) {
        return topk;
    }
    auto cursors = index.block_max_scored_cursors(gsl::make_span(term_ids), scorer);
    if (query.threshold()) {
        topk.set_threshold(*query.threshold());
    }
    auto joined = join_block_max_wand(std::move(cursors), 0.0F, accumulate::Add{}, [&](auto score) {
        return topk.would_enter(score);
    });
    v1::for_each(joined, [&](auto& cursor) { topk.insert(cursor.payload(), cursor.value()); });
    return topk;
}

} // namespace pisa::v1
