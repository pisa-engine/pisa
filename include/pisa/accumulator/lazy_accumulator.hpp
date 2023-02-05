#pragma once

#include <algorithm>
#include <array>
#include <cstddef>
#include <vector>

#include "concepts.hpp"
#include "partial_score_accumulator.hpp"
#include "topk_queue.hpp"

namespace pisa {

/**
 * Lazy accumulator fully resets the entire array only (1 << counter_bit_size) call to `reset()`.
 * For example, if `counter_bit_size = 3`, then all values are set to 0 every 8th reset. To allow
 * for that, the array is partitioned into blocks, each of which has a number of accumulators and
 * a descriptor that encodes when was the last time the block was in use. If it was used before
 * the current query (according to a counter that is reset each cycle), the block is wiped out
 * before accumulating another score.
 */
template <int counter_bit_size, typename Descriptor = std::uint64_t>
class LazyAccumulator {
    using reference = float&;

    static_assert(
        std::is_integral_v<Descriptor> && std::is_unsigned_v<Descriptor>, "must be unsigned number");
    constexpr static auto descriptor_size_in_bits = sizeof(Descriptor) * 8;
    constexpr static auto counters_in_descriptor = descriptor_size_in_bits / counter_bit_size;
    constexpr static auto cycle = (1U << counter_bit_size);
    constexpr static Descriptor mask = (1U << counter_bit_size) - 1;

    struct Block {
        Descriptor descriptor{};
        std::array<float, counters_in_descriptor> accumulators{};

        [[nodiscard]] auto counter(int pos) const noexcept -> int
        {
            if constexpr (counter_bit_size == 8) {  // NOLINT(readability-braces-around-statements)
                return static_cast<int>(*(reinterpret_cast<uint8_t const*>(&descriptor) + pos));
            } else {
                return (descriptor >> (pos * counter_bit_size)) & mask;
            }
        }

        void reset_counter(int pos, int counter)
        {
            if constexpr (counter_bit_size == 8) {  // NOLINT(readability-braces-around-statements)
                *(reinterpret_cast<uint8_t*>(&descriptor) + pos) = static_cast<uint8_t>(counter);
            } else {
                auto const shift = pos * counter_bit_size;
                descriptor &= ~(mask << shift);
                descriptor |= static_cast<Descriptor>(counter) << shift;
            }
            accumulators[pos] = 0;
        }
    };

  public:
    explicit LazyAccumulator(std::size_t size)
        : m_size(size), m_accumulators((size + counters_in_descriptor - 1) / counters_in_descriptor)
    {
        PISA_ASSERT_CONCEPT(PartialScoreAccumulator<decltype(*this)>);
    }

    void reset()
    {
        if (m_counter == 0) {
            auto first = reinterpret_cast<std::byte*>(&m_accumulators.front());
            auto last =
                std::next(reinterpret_cast<std::byte*>(&m_accumulators.back()), sizeof(Block));
            std::fill(first, last, std::byte{0});
        }
    }

    void accumulate(std::size_t document, float score)
    {
        auto const block = document / counters_in_descriptor;
        auto const pos_in_block = document % counters_in_descriptor;
        if (m_accumulators[block].counter(pos_in_block) != m_counter) {
            m_accumulators[block].reset_counter(pos_in_block, m_counter);
        }
        m_accumulators[block].accumulators[pos_in_block] += score;
    }

    void collect(topk_queue& topk)
    {
        uint64_t docid = 0U;
        for (auto const& block: m_accumulators) {
            int pos = 0;
            for (auto const& score: block.accumulators) {
                if (block.counter(pos++) == m_counter && topk.would_enter(score)) {
                    topk.insert(score, docid);
                }
                ++docid;
            }
        };
        m_counter = (m_counter + 1) % cycle;
    }

    [[nodiscard]] auto size() const noexcept -> std::size_t { return m_size; }

  private:
    std::size_t m_size;
    std::vector<Block> m_accumulators;
    int m_counter{};
};

}  // namespace pisa
