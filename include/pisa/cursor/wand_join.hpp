#pragma once

#include <algorithm>
#include <numeric>

#include <gsl/span>
#include <range/v3/algorithm/find_if_not.hpp>
#include <range/v3/algorithm/sort.hpp>

#include "util/compiler_attribute.hpp"
#include "util/likely.hpp"

namespace pisa {

template <
    typename Cursors,
    typename Payload,
    typename AccumulateFn,
    typename ThresholdFn
    // typename Inspect = void
    >
struct BlockMaxWandJoin {
    using cursor_type = std::decay_t<typename Cursors::value_type>;
    using payload_type = Payload;
    using value_type = std::uint32_t;

    BlockMaxWandJoin(
        Cursors cursors,
        Payload init,
        AccumulateFn accumulate,
        ThresholdFn above_threshold,
        std::uint32_t sentinel)
        : m_cursors(std::move(cursors)),
          m_init(std::move(init)),
          m_accumulate(std::move(accumulate)),
          m_above_threshold(std::move(above_threshold))
    {
        m_ordered_cursors.reserve(m_cursors.size());
        std::transform(
            m_cursors.begin(),
            m_cursors.end(),
            std::back_inserter(m_ordered_cursors),
            [](auto& cursor) { return &cursor; });
        sort_cursors();

        // TODO(michal): automatic sentinel inference.
        m_sentinel = sentinel;
        next();
    }

    [[nodiscard]] constexpr PISA_ALWAYSINLINE auto docid() const noexcept -> value_type
    {
        return m_current_value;
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
            auto upper_bound = 0.0F;
            std::size_t pivot = 0;
            bool found_pivot = false;
            for (; pivot < m_ordered_cursors.size(); ++pivot) {
                if (m_ordered_cursors[pivot]->docid() >= m_sentinel) {
                    break;
                }
                upper_bound += m_ordered_cursors[pivot]->max_score();
                if (m_above_threshold(upper_bound)) {
                    found_pivot = true;
                    auto pivot_docid = m_ordered_cursors[pivot]->docid();
                    for (; pivot + 1 < m_ordered_cursors.size()
                         && m_ordered_cursors[pivot + 1]->docid() == pivot_docid;
                         ++pivot) {
                    }
                    break;
                }
            }
            if (not found_pivot) {
                m_current_value = sentinel();
                return;
            }

            auto pivot_docid = m_ordered_cursors[pivot]->docid();
            double block_upper_bound = 0;
            for (auto cursor_iter = m_ordered_cursors.begin();
                 cursor_iter != std::next(m_ordered_cursors.begin(), pivot + 1);
                 ++cursor_iter) {
                auto& cursor = *cursor_iter;
                if (cursor->block_max_docid() < pivot_docid) {
                    cursor->block_max_next_geq(pivot_docid);
                }
                block_upper_bound += cursor->block_max_score() * cursor->query_weight();
            }

            if (m_above_threshold(block_upper_bound)) {
                if (pivot_docid == m_ordered_cursors.front()->docid()) {
                    m_current_payload = m_init;
                    m_current_value = pivot_docid;
                    for (auto* en: m_ordered_cursors) {
                        if (en->docid() != pivot_docid) {
                            break;
                        }
                        float part_score = en->score();
                        m_current_payload += part_score;
                        block_upper_bound -= en->block_max_score() * en->query_weight() - part_score;
                        if (!m_above_threshold(block_upper_bound)) {
                            break;
                        }
                    }
                    for (auto* en: m_ordered_cursors) {
                        if (en->docid() != pivot_docid) {
                            break;
                        }
                        en->next();
                    }
                    sort_cursors();
                    exit = true;
                } else {
                    uint64_t next_list = pivot;
                    for (; m_ordered_cursors[next_list]->docid() == pivot_docid; --next_list) {
                    }
                    m_ordered_cursors[next_list]->next_geq(pivot_docid);

                    for (size_t i = next_list + 1; i < m_ordered_cursors.size(); ++i) {
                        if (m_ordered_cursors[i]->docid() <= m_ordered_cursors[i - 1]->docid()) {
                            std::swap(m_ordered_cursors[i], m_ordered_cursors[i - 1]);
                        } else {
                            break;
                        }
                    }
                    exit = false;
                }
            } else {
                move_on(pivot, pivot_docid);
                exit = false;
            }
        }
    }

    [[nodiscard]] constexpr PISA_ALWAYSINLINE auto empty() const noexcept -> bool
    {
        return m_current_value >= sentinel();
    }

  private:
    PISA_ALWAYSINLINE void sort_cursors()
    {
        std::sort(m_ordered_cursors.begin(), m_ordered_cursors.end(), [](auto* lhs, auto* rhs) {
            return lhs->docid() < rhs->docid();
        });
    }

    PISA_ALWAYSINLINE void move_on(std::size_t pivot, value_type pivot_docid)
    {
        uint64_t next_list = pivot;

        float max_weight = m_ordered_cursors[next_list]->max_score();

        for (uint64_t i = 0; i < pivot; i++) {
            if (m_ordered_cursors[i]->max_score() > max_weight) {
                next_list = i;
                max_weight = m_ordered_cursors[i]->max_score();
            }
        }

        auto next = m_sentinel;

        for (size_t i = 0; i <= pivot; ++i) {
            if (m_ordered_cursors[i]->block_max_docid() < next) {
                next = m_ordered_cursors[i]->block_max_docid();
            }
        }

        next = next + 1;
        if (pivot + 1 < m_ordered_cursors.size() && m_ordered_cursors[pivot + 1]->docid() < next) {
            next = m_ordered_cursors[pivot + 1]->docid();
        }

        if (next <= pivot_docid) {
            next = pivot_docid + 1;
        }

        m_ordered_cursors[next_list]->next_geq(next);

        // bubble down the advanced list
        for (size_t i = next_list + 1; i < m_ordered_cursors.size(); ++i) {
            if (m_ordered_cursors[i]->docid() < m_ordered_cursors[i - 1]->docid()) {
                std::swap(m_ordered_cursors[i], m_ordered_cursors[i - 1]);
            } else {
                break;
            }
        }
    }

    Cursors m_cursors;
    payload_type m_init;
    AccumulateFn m_accumulate;
    ThresholdFn m_above_threshold;

    std::vector<cursor_type*> m_ordered_cursors;
    value_type m_current_value{};
    value_type m_sentinel{};
    payload_type m_current_payload{};
};

template <typename Cursors, typename Payload, typename AccumulateFn, typename ThresholdFn>
auto join_block_max_wand(
    Cursors cursors, Payload init, AccumulateFn accumulate, ThresholdFn threshold, std::uint32_t sentinel)
{
    return BlockMaxWandJoin<Cursors, Payload, AccumulateFn, ThresholdFn>(
        std::move(cursors), std::move(init), std::move(accumulate), std::move(threshold), sentinel);
}

}  // namespace pisa
