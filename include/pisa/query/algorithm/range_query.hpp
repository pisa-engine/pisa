#pragma once

#include <gsl/span>

#include "query/queries.hpp"
#include "topk_queue.hpp"

namespace pisa {

template <typename QueryAlg>
struct range_query {

    range_query(uint64_t k)
        : m_k(k), m_topk(k) {}

    template <typename Cursor>
    uint64_t operator()(gsl::span<Cursor> cursors, uint64_t max_docid, size_t range_size)
    {
        m_topk.clear();
        if (cursors.empty()) {
            return 0;
        }

        for (size_t end = range_size;
             end + range_size <= max_docid; end += range_size) {
            process_range(cursors, end);
        }
        process_range(cursors, max_docid);

        m_topk.finalize();
        return m_topk.topk().size();
    }

    std::vector<std::pair<float, uint64_t>> const &topk() const { return m_topk.topk(); }

    template <typename Cursor>
    void process_range(gsl::span<Cursor> cursors, size_t end)
    {
        QueryAlg query_alg(m_k);
        query_alg(cursors, end);
        auto small_topk = query_alg.topk();
        for (const auto &entry : small_topk) {
            m_topk.insert(entry.first, entry.second);
        }
    }

   private:
    uint64_t m_k;
    TopKQueue m_topk;
};

} // namespace pisa
