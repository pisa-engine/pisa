#pragma once

#include <algorithm>
#include <cstdint>
#include <utility>
#include <vector>

namespace pisa {

using Score = float;

// NOTE: 64-bits are used to comply with `topk_queue` interface. It should be changed at some point,
// both here, and in all algorithms.
using DocId = std::uint64_t;

/// Accumulates all document scores that are above a pre-set threshold.
///
/// As opposed to `topk_queue`, there is no set capacity, and the threshold never changes,
/// unless done explicitly through `clear` (set to 0) or `set_threshold`.
struct ThresholdAccumulator {
    using entry_type = std::pair<Score, DocId>;

    explicit ThresholdAccumulator(Score t) : m_threshold(t) {}
    ThresholdAccumulator(ThresholdAccumulator const&) = default;
    ThresholdAccumulator(ThresholdAccumulator&&) noexcept = default;
    ThresholdAccumulator& operator=(ThresholdAccumulator const&) = default;
    ThresholdAccumulator& operator=(ThresholdAccumulator&&) noexcept = default;
    ~ThresholdAccumulator() = default;

    auto insert(Score score, DocId docid) -> bool
    {
        if (would_enter(score)) {
            m_entries.emplace_back(score, docid);
            return true;
        }
        return false;
    }

    void finalize() { std::sort(m_entries.begin(), m_entries.end(), std::greater<>()); }

    void set_threshold(Score t) noexcept { m_threshold = t; }

    void clear() noexcept
    {
        m_entries.clear();
        m_threshold = 0;
    }

    [[nodiscard]] auto would_enter(float score) const noexcept -> bool
    {
        return score >= m_threshold;
    }
    [[nodiscard]] auto threshold() const noexcept -> Score { return m_threshold; }
    [[nodiscard]] auto size() const noexcept -> std::size_t { return m_entries.size(); }
    [[nodiscard]] auto topk() const -> std::vector<std::pair<Score, DocId>> const&
    {
        return m_entries;
    }

  private:
    Score m_threshold;
    std::vector<entry_type> m_entries;
};

}  // namespace pisa
