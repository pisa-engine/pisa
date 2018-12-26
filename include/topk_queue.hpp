#pragma once

namespace ds2i {

struct topk_queue {
    using entry_type = std::pair<float, uint64_t>;

    explicit topk_queue(uint64_t k) : m_threshold(0), m_k(k) { m_q.reserve(k + 1); }

    topk_queue(const topk_queue &q) : m_threshold(q.m_threshold), m_k(q.m_k), m_q(q.m_q) {
        m_k = q.m_k;
        m_threshold = q.m_threshold;
    }

    topk_queue &operator=(const topk_queue &q) {
        m_k = q.m_k;
        m_threshold = q.m_threshold;
        return *this;
    }

    [[nodiscard]] constexpr static auto min_heap_order(entry_type const &lhs,
                                                       entry_type const &rhs) noexcept -> bool {
        return lhs.first > rhs.first;
    }

    bool insert(float score) { return insert(score, 0); }

    bool insert(float score, uint64_t docid) {
        if (DS2I_UNLIKELY(score < m_threshold)) {
            return false;
        }
        m_q.emplace_back(score, docid);
        if (DS2I_UNLIKELY(m_q.size() < m_k)) {
            std::push_heap(m_q.begin(), m_q.end(), min_heap_order);
            if(DS2I_UNLIKELY(m_q.size() == m_k)) {
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
        size_t size =
            std::lower_bound(m_q.begin(),
                             m_q.end(),
                             0,
                             [](std::pair<float, uint64_t> l, float r) { return l.first > r; }) -
            m_q.begin();
        m_q.resize(size);
    }

    [[nodiscard]] std::vector<entry_type> const &topk() const noexcept { return m_q; }

    void set_threshold(float t) {
        for (size_t i = 0; i < m_k; ++i) {
            insert(0);
        }
        m_threshold = t;
    }

    void clear() noexcept { m_q.clear(); }

    [[nodiscard]] uint64_t size() const noexcept { return m_k; }

    float                   m_threshold;
    uint64_t                m_k;
    std::vector<entry_type> m_q;
};

} // namespace ds2i
