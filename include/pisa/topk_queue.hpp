#pragma once

#include "type_alias.hpp"
#include "util/likely.hpp"
#include "util/util.hpp"

#include <algorithm>

namespace pisa {

struct topk_queue {
    using entry_type = std::pair<Score, DocId>;

    explicit topk_queue(std::size_t k) : m_threshold(0), m_k(k) { m_q.reserve(m_k + 1); }
    topk_queue(topk_queue const&) = default;
    topk_queue(topk_queue&&) noexcept = default;
    topk_queue& operator=(topk_queue const&) = default;
    topk_queue& operator=(topk_queue&&) noexcept = default;
    ~topk_queue() = default;

    /// Min-heap ordering on entry scores.
    [[nodiscard]] constexpr static auto
    min_heap_order(entry_type const& lhs, entry_type const& rhs) noexcept -> bool
    {
        return lhs.first > rhs.first;
    }

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
                m_threshold = m_q.front().first;
            }
        } else {
            std::pop_heap(m_q.begin(), m_q.end(), min_heap_order);
            m_q.pop_back();
            m_threshold = m_q.front().first;
        }
        return true;
    }

    /// Checks if the given score would enter the queue.
    [[nodiscard]] auto would_enter(Score score) const noexcept -> bool
    {
        return score >= m_threshold;
    }

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

    /// Updates the current threshold.
    void set_threshold(Score t) noexcept { m_threshold = t; }

    /// Returns the current threshold.
    [[nodiscard]] auto threshold() const noexcept -> Score { return m_threshold; }

    /// Empties the queue and resets the threshold to 0.
    void clear() noexcept
    {
        m_q.clear();
        m_threshold = 0;
    }

    /// Returns the capacity of the queue, i.e., k in top-k.
    [[nodiscard]] size_t capacity() const noexcept { return m_k; }

    /// Returns the number of elements currently in the queue.
    [[nodiscard]] size_t size() const noexcept { return m_q.size(); }

  private:
    Score m_threshold;
    std::size_t m_k;
    std::vector<entry_type> m_q;
};

}  // namespace pisa
