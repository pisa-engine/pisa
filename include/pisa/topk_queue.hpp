#pragma once

#include "type_alias.hpp"
#include "util/likely.hpp"
#include "util/util.hpp"

#include <algorithm>

namespace pisa {

/// Top-k document priority queue.
///
/// Accumulates (document, score) pairs during a retrieval algorithm.
/// This is a min heap; once it is full (contains k elements), any new entry
/// with a score higher than the one on the top of the heap will replace the
/// min element. Because it is a binary heap, the elements are not sorted;
/// use `finalize()` member function to sort it before accessing it with
/// `topk()`.
struct topk_queue {
    using entry_type = std::pair<Score, DocId>;

    /// Constructs a top-k priority queue with the given intitial threshold.
    ///
    /// Note that if the initial threshold is in fact higher than otherwise
    /// the k-th highest score would be, then some top-k result will be missing
    /// from the final result, replaced by lower-scoring documents.
    explicit topk_queue(std::size_t k, Score initial_threshold = 0.0F)
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

    /// Inserts a heap entry.
    ///
    /// Attempts to inserts an entry with the given score and docid. If the score
    /// is below the threshold, the entry will **not** be inserted, and `false`
    /// will be returned. Otherwise, the entry will be inserted, and `true` returned.
    /// If the heap is full, the entry with the lowest value will be removed, i.e.,
    /// the heap will maintain its size.
    auto insert(Score score, DocId docid = 0) -> bool
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

    /// Sorts the results in the heap container in the descending score order.
    ///
    /// After calling this function, the heap should be no longer modified, as
    /// the heap order will not be preserved.
    void finalize()
    {
        std::sort_heap(m_q.begin(), m_q.end(), min_heap_order);
        size_t size = std::lower_bound(
                          m_q.begin(),
                          m_q.end(),
                          0,
                          [](std::pair<Score, DocId> l, Score r) { return l.first > r; })
            - m_q.begin();
        m_q.resize(size);
    }

    /// Returns a reference to the contents of the heap.
    ///
    /// This is intended to be used after calling `finalize()` first, which will sort
    /// the results in order of descending scores.
    [[nodiscard]] std::vector<entry_type> const& topk() const noexcept { return m_q; }

    /// Returns the threshold based on the heap state, defined as the score of the `k`-th document,
    /// or 0.0 if the heap is not full.
    [[nodiscard]] auto true_threshold() const noexcept -> Score
    {
        return capacity() == size() ? m_q.front().first : 0.0;
    }
    /// Returns the threshold set at the start (by default 0.0).
    [[nodiscard]] auto initial_threshold() const noexcept -> Score { return m_initial_threshold; }

    /// Returns the maximum of `true_threshold()` and `initial_threshold()`.
    [[nodiscard]] auto effective_threshold() const noexcept -> Score
    {
        return m_effective_threshold;
    }

    /// Returns `true` if no documents have been missed up to this point.
    /// The reason why document could be missed is forcing a threshold that is too high
    /// (overestimated).
    [[nodiscard]] auto is_safe() noexcept -> bool
    {
        return m_effective_threshold >= m_initial_threshold;
    }

    /// Empties the queue and resets the threshold to 0 (or the given value).
    void clear(Score initial_threshold = 0.0) noexcept
    {
        m_q.clear();
        m_effective_threshold = std::nextafter(m_initial_threshold, 0.0);
        m_initial_threshold = initial_threshold;
    }

    /// The maximum number of entries that can fit in the queue.
    [[nodiscard]] auto capacity() const noexcept -> std::size_t { return m_k; }

    /// The current number of entries in the queue.
    [[nodiscard]] auto size() const noexcept -> std::size_t { return m_q.size(); }

  private:
    [[nodiscard]] constexpr static auto
    min_heap_order(entry_type const& lhs, entry_type const& rhs) noexcept -> bool
    {
        return lhs.first > rhs.first;
    }

    std::size_t m_k;
    float m_initial_threshold;
    std::vector<entry_type> m_q;
    float m_effective_threshold;
};

}  // namespace pisa
