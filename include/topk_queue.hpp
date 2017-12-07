#pragma once
namespace ds2i {

struct topk_queue {
    topk_queue(uint64_t k) : m_k(k) {}

    topk_queue(const topk_queue &q) : m_q(q.m_q) {
        m_k = q.m_k;
        threshold = q.threshold;
    }

    bool insert(float score) { return insert(score, 0); }

    bool insert(float score, uint64_t docid) {
        if (m_q.size() < m_k) {
            m_q.push_back(std::make_pair(score, docid));
            std::push_heap(m_q.begin(),
                           m_q.end(),
                           [](std::pair<float, uint64_t> l, std::pair<float, uint64_t> r) {
                               return l.first > r.first;
                           });
            threshold = m_q.front().first;
            return true;
        } else {
            if (score > threshold) {
                std::pop_heap(m_q.begin(),
                              m_q.end(),
                              [](std::pair<float, uint64_t> l, std::pair<float, uint64_t> r) {
                                  return l.first > r.first;
                              });
                m_q.back() = std::make_pair(score, docid);
                std::push_heap(m_q.begin(),
                               m_q.end(),
                               [](std::pair<float, uint64_t> l, std::pair<float, uint64_t> r) {
                                   return l.first > r.first;
                               });
                threshold = m_q.front().first;
                return true;
            }
        }
        return false;
    }

    bool would_enter(float score) const { return m_q.size() < m_k || score > threshold; }

    void finalize() {
        std::sort_heap(
            m_q.begin(), m_q.end(), [](std::pair<float, uint64_t> l, std::pair<float, uint64_t> r) {
                return l.first > r.first;
            });
        size_t size =
            std::lower_bound(m_q.begin(),
                             m_q.end(),
                             0,
                             [](std::pair<float, uint64_t> l, float r) { return l.first > r; }) -
            m_q.begin();
        m_q.resize(size);
    }

    void sort_docid() {
        std::sort_heap(
            m_q.begin(), m_q.end(), [](std::pair<float, uint64_t> l, std::pair<float, uint64_t> r) {
                return l.second < r.second;
            });
    }

    std::vector<std::pair<float, uint64_t>> const &topk() const { return m_q; }

    void set_threshold(float t) {
        for (size_t i = 0; i < m_k; ++i) {
            insert(0);
        }
        threshold = t;
    }

    void clear() { m_q.clear(); }

    uint64_t size() { return m_k; }

    float threshold;
    uint64_t m_k;
    std::vector<std::pair<float, uint64_t>> m_q;
};
} // namespace ds2i