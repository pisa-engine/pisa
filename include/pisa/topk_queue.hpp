#pragma once

#include <algorithm>

#include "util/likely.hpp"
#include "util/util.hpp"

namespace pisa {

using Threshold = float;

/// Top-k document priority queue.
///
/// Accumulates (document, score) pairs during a retrieval algorithm.
/// This is a min heap; once it is full (contains k elements), any new entry
/// with a score higher than the one on the top of the heap will replace the
/// min element. Because it is a binary heap, the elements are not sorted;
/// use `finalize()` member function to sort it before accessing it with
/// `topk()`.
struct topk_queue {
    using entry_type = std::pair<float, uint64_t>;

    /// Constructs a top-k priority queue with the given intitial threshold.
    ///
    /// Note that if the initial threshold is in fact higher than otherwise
    /// the k-th highest score would be, then some top-k result will be missing
    /// from the final result, replaced by lower-scoring documents.
    explicit topk_queue(uint64_t k, Threshold initial_threshold = 0.0F)
        : m_k(k), m_initial_threshold(initial_threshold)
    {
        m_effective_threshold = std::nextafter(m_initial_threshold, 0.0F);
        m_q.reserve(m_k + 1);
    }
    topk_queue(topk_queue const&) = default;
    topk_queue(topk_queue&&) noexcept = default;
    topk_queue& operator=(topk_queue const&) = default;
    topk_queue& operator=(topk_queue&&) noexcept = default;
    ~topk_queue() = default;

    /// Inserts an entry with the given score and document ID to the queue.
    ///
    /// If the score is below the current threshold, the entry will not be inserted.
    /// This includes a case in which the document would be inserted but the threshold
    /// was manually set to a higher value.
    bool insert(float score, uint64_t docid = 0)
    {
        if (PISA_UNLIKELY(not would_enter(score))) {
            return false;
        }
        m_q.emplace_back(score, docid);
        if (PISA_UNLIKELY(m_q.size() <= m_k)) {
            std::push_heap(m_q.begin(), m_q.end(), min_heap_order);
            if (PISA_UNLIKELY(m_q.size() == m_k)) {
                m_effective_threshold = m_q.front().first;
            }
        } else {
            std::pop_heap(m_q.begin(), m_q.end(), min_heap_order);
            m_q.pop_back();
            m_effective_threshold = m_q.front().first;
        }
        return true;
    }

    /// Checks if an entry with the given score would be inserted to the queue, according
    /// to the current threshold.
    bool would_enter(float score) const { return score > m_effective_threshold; }

    /// Sorts all entries in order of decreasing scores.
    void finalize()
    {
        std::sort_heap(m_q.begin(), m_q.end(), min_heap_order);
        size_t size = std::lower_bound(
                          m_q.begin(),
                          m_q.end(),
                          0,
                          [](std::pair<float, uint64_t> l, float r) { return l.first > r; })
            - m_q.begin();
        m_q.resize(size);
    }

    /// Returns the elements in the queue. Typically, this should be called after `finalize`
    /// to retrieve the final results in the correct order.
    [[nodiscard]] std::vector<entry_type> const& topk() const noexcept { return m_q; }

    /// Returns the threshold based on the heap state, defined as the score of the `k`-th document,
    /// or 0.0 if the heap is not full.
    Threshold true_threshold() const noexcept
    {
        return capacity() == size() ? m_q.front().first : 0.0;
    }

    /// Returns the threshold set at the start (by default 0.0).
    Threshold initial_threshold() const noexcept { return m_initial_threshold; }

    /// Returns the maximum of `true_threshold()` and `initial_threshold()`.
    Threshold effective_threshold() const noexcept { return m_effective_threshold; }

    /// Returns `true` if no documents have been missed up to this point.
    /// The reason why document could be missed is forcing a threshold that is too high
    /// (overestimated).
    bool is_safe() noexcept { return m_effective_threshold >= m_initial_threshold; }

    void clear(Threshold initial_threshold = 0.0) noexcept
    {
        m_q.clear();
        m_effective_threshold = std::nextafter(m_initial_threshold, 0.0);
        m_initial_threshold = initial_threshold;
    }

    /// The maximum number of entries that can fit in the queue.
    [[nodiscard]] size_t capacity() const noexcept { return m_k; }

    /// The current number of entries in the queue.
    [[nodiscard]] size_t size() const noexcept { return m_q.size(); }

  private:
    [[nodiscard]] constexpr static auto
    min_heap_order(entry_type const& lhs, entry_type const& rhs) noexcept -> bool
    {
        return lhs.first > rhs.first;
    }

    uint64_t m_k;
    float m_initial_threshold;
    std::vector<entry_type> m_q;
    float m_effective_threshold;
};

}  // namespace pisa
