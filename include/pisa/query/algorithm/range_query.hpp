#pragma once

#include "topk_queue.hpp"
#include "query/queries.hpp"

namespace pisa {

template<typename QueryAlg>
struct range_query {

    range_query(uint64_t k, uint64_t max_docid, uint64_t range_size)
        : m_k(k), m_topk(k), m_max_docid(max_docid), m_range_size(range_size) {}

    template<typename CursorRange>
    uint64_t operator()(CursorRange &&cursors) {
        m_topk.clear();
        if (cursors.empty()) {
            return 0;
        }

        for (size_t end = m_range_size; end < m_max_docid; end += m_range_size) {
            QueryAlg queryAlg(m_k, end);
            queryAlg(cursors);
            auto small_topk = queryAlg.topk();
            for (const auto& entry : small_topk) {
                m_topk.insert(entry.first, entry.second);
            }
        }
        return m_topk.size();


        m_topk.finalize();
        return m_topk.topk().size();
    }

    std::vector<std::pair<float, uint64_t>> const &topk() const { return m_topk.topk(); }

   private:
    uint64_t        m_k;
    topk_queue      m_topk;
    uint64_t        m_max_docid;
    uint64_t        m_range_size;
};

} // namespace pisa