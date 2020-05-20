#pragma once

#include <iostream>
#include <iterator>
#include <numeric>
#include <optional>

#include <gsl/span>

#include "cursor/cursor.hpp"
#include "cursor/cursor_union.hpp"
#include "util/compiler_attribute.hpp"
#include "util/likely.hpp"

namespace pisa {

template <typename CursorContainer, typename Payload, typename AccumulateFn, typename ThresholdFn>
struct BlockMaxCursorUnion: CursorJoin<typename CursorContainer::value_type, Payload, AccumulateFn> {
    using Cursor = typename CursorContainer::value_type;
    using iterator_category =
        typename std::iterator_traits<typename CursorContainer::iterator>::iterator_category;
    static_assert(
        std::is_base_of<std::random_access_iterator_tag, iterator_category>(),
        "cursors must be stored in a random access container");
    using value_type = std::uint32_t;
    using cursor_join_type = CursorJoin<typename CursorContainer::value_type, Payload, AccumulateFn>;

    constexpr BlockMaxCursorUnion(
        CursorContainer cursors,
        Payload init,
        AccumulateFn accumulate,
        ThresholdFn above_threshold,
        std::optional<value_type> sentinel)
        : cursor_join_type(std::move(init), std::move(accumulate)),
          m_cursors(std::move(cursors)),
          m_above_threshold(std::move(above_threshold))
    {
        this->init_payload();
        if (m_cursors.empty()) {
            this->set_current_value(std::numeric_limits<value_type>::max());
        } else {
            m_next_docid =
                std::min_element(m_cursors.begin(), m_cursors.end(), [](auto const& lhs, auto const& rhs) {
                    return lhs.docid() < rhs.docid();
                })->docid();
            if (sentinel) {
                this->set_sentinel(*sentinel);
            } else {
                auto max_sentinel = std::max_element(
                                        m_cursors.begin(),
                                        m_cursors.end(),
                                        [](auto const& lhs, auto const& rhs) {
                                            return lhs.universe() < rhs.universe();
                                        })
                                        ->universe();
                this->set_sentinel(max_sentinel);
            }
            next();
            m_block_max_upper_bound = std::accumulate(
                m_cursors.begin(), m_cursors.end(), 0.0F, [](auto acc, auto& cursor) {
                    return acc + cursor.block_max_score();
                });
        }
    }

    constexpr PISA_ALWAYSINLINE void next()
    {
        if (PISA_UNLIKELY(m_next_docid == this->sentinel())) {
            this->set_current_value(this->sentinel());
            this->init_payload();
        } else {
            while (not m_above_threshold(m_block_max_upper_bound)) {
                m_next_docid = this->sentinel();
                auto cursor_to_move = 0;
                auto idx = 0;
                for (auto& cursor: m_cursors) {
                    if (cursor.docid() < this->sentinel() && cursor.block_max_docid() < m_next_docid) {
                        m_next_docid = cursor.block_max_docid();
                        cursor_to_move = idx;
                    }
                    ++idx;
                }
                if (PISA_UNLIKELY(m_next_docid == this->sentinel())) {
                    this->set_current_value(this->sentinel());
                    this->init_payload();
                    return;
                }
                m_next_docid += 1;
                m_block_max_upper_bound -= m_cursors[cursor_to_move].block_max_score();
                m_cursors[cursor_to_move].next_geq(m_next_docid);
                m_cursors[cursor_to_move].block_max_next_geq(m_next_docid);
                m_block_max_upper_bound += m_cursors[cursor_to_move].block_max_score();
            }
            this->init_payload();
            this->set_current_value(m_next_docid);
            m_next_docid = this->sentinel();
            for (auto& cursor: m_cursors) {
                if (cursor.docid() == this->docid()) {
                    this->accumulate(cursor);
                    cursor.next();
                    m_block_max_upper_bound -= cursor.block_max_score();
                    cursor.block_max_next_geq(cursor.docid());
                    m_block_max_upper_bound += cursor.block_max_score();
                }
                if (auto value = cursor.docid(); value < m_next_docid) {
                    m_next_docid = value;
                }
            }
        }
    }

  private:
    CursorContainer m_cursors;
    ThresholdFn m_above_threshold;

    float m_block_max_upper_bound{};
    std::uint32_t m_next_docid{};
};

template <typename CursorContainer, typename Payload, typename AccumulateFn, typename ThresholdFn>
[[nodiscard]] constexpr inline auto block_max_union(
    CursorContainer cursors,
    Payload init,
    AccumulateFn accumulate,
    ThresholdFn above_threshold,
    std::optional<std::uint32_t> sentinel = std::nullopt)
{
    return BlockMaxCursorUnion<CursorContainer, Payload, AccumulateFn, ThresholdFn>(
        std::move(cursors), std::move(init), std::move(accumulate), std::move(above_threshold), sentinel);
}

}  // namespace pisa
