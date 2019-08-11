#pragma once

#include <algorithm>
#include <cstdint>
#include <vector>

#include "util/likely.hpp"

namespace pisa {

/* struct TopKQueue { */
/*     using DocID = std::uint64_t; */
/*     using Score = float; */
/*     using Entry = std::pair<Score, DocID>; */

/*     explicit TopKQueue(std::size_t capacity); */
/*     TopKQueue(TopKQueue const &) = default; */
/*     TopKQueue(TopKQueue &&) = default; */
/*     TopKQueue& operator=(TopKQueue const &) = default; */
/*     TopKQueue& operator=(TopKQueue &&) = default; */

/*     auto insert(Score score, DocID docid = 0) -> bool */
/*     { */
/*         if (PISA_UNLIKELY(score <= m_threshold)) { */
/*             return false; */
/*         } */
/*         m_entries.emplace_back(score, docid); */
/*         if (PISA_UNLIKELY(m_entries.size() <= m_k)) { */
/*             std::push_heap(m_entries.begin(), m_entries.end(), min_heap_order); */
/*             if(PISA_UNLIKELY(m_entries.size() == m_k)) { */
/*                 m_threshold = m_entries.front().first; */
/*             } */
/*         } else { */
/*             std::pop_heap(m_entries.begin(), m_entries.end(), min_heap_order); */
/*             m_entries.pop_back(); */
/*             m_threshold = m_entries.front().first; */
/*         } */
/*         return true; */
/*     } */

/*     [[nodiscard]] auto topk() const noexcept -> std::vector<Entry> const &; */
/*     [[nodiscard]] auto would_enter(Score score) const -> bool; */

/*     void finalize(); */
/*     void clear() noexcept; */

/*     [[nodiscard]] uint64_t size() const noexcept { return m_k; } */

/*    private: */
/*     [[nodiscard]] constexpr static auto min_heap_order(entry_type const &lhs, */
/*                                                        entry_type const &rhs) noexcept -> bool */
/*     { */
/*         return lhs.first > rhs.first; */
/*     } */

/*    private: */
/*     Score m_threshold; */
/*     std::size_t m_capacity; */
/*     std::vector<Entry> m_entries; */
/* }; */

struct topk_queue {
    using entry_type = std::pair<float, std::uint64_t>;

    explicit topk_queue(std::uint64_t k) : m_threshold(0), m_k(k) { m_q.reserve(m_k + 1); }
    topk_queue(topk_queue const &q) = default;
    topk_queue &operator=(topk_queue const &q) = default;

    [[nodiscard]] constexpr static auto min_heap_order(entry_type const &lhs,
                                                       entry_type const &rhs) noexcept -> bool {
        return lhs.first > rhs.first;
    }

    bool insert(float score) { return insert(score, 0); }

    bool insert(float score, std::uint64_t docid)
    {
        if (PISA_UNLIKELY(score <= m_threshold)) {
            return false;
        }
        m_q.emplace_back(score, docid);
        if (PISA_UNLIKELY(m_q.size() <= m_k)) {
            std::push_heap(m_q.begin(), m_q.end(), min_heap_order);
            if(PISA_UNLIKELY(m_q.size() == m_k)) {
                m_threshold = m_q.front().first;
            }
        } else {
            std::pop_heap(m_q.begin(), m_q.end(), min_heap_order);
            m_q.pop_back();
            m_threshold = m_q.front().first;
        }
        return true;
    }

    bool would_enter(float score) const { return m_q.size() < m_k || score > m_threshold; }

    void finalize() {
        std::sort_heap(m_q.begin(), m_q.end(), min_heap_order);
        std::size_t size =
            std::lower_bound(m_q.begin(),
                             m_q.end(),
                             0,
                             [](std::pair<float, std::uint64_t> l, float r) { return l.first > r; })
            - m_q.begin();
        m_q.resize(size);
    }

    [[nodiscard]] std::vector<entry_type> const &topk() const noexcept { return m_q; }

    void clear() noexcept {
        m_q.clear();
        m_threshold = 0;
    }

    [[nodiscard]] std::uint64_t size() const noexcept { return m_k; }

   private:
    float                   m_threshold;
    std::uint64_t m_k;
    std::vector<entry_type> m_q;
};

} // namespace pisa
