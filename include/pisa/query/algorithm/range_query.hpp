#pragma once

#include "cursor/block_max_scored_cursor.hpp"
#include "query/queries.hpp"
#include "topk_queue.hpp"
#include "wand_data_range.hpp"

namespace pisa {
using WandTypeRange = wand_data_range<128, 1024, bm25>;

template <typename QueryAlg, size_t block_size = 128>
struct range_query {

    range_query(QueryAlg algorithm, uint64_t k)
        : m_k(k), m_topk(k), m_algorithm(std::move(algorithm))
    {}

    template <typename CursorRange>
    uint64_t operator()(CursorRange &&cursors, uint64_t max_docid, size_t range_size)
    {
        m_topk.clear();
        if (cursors.empty()) {
            return 0;
        }

        for (size_t end = range_size; end + range_size <= max_docid; end += range_size) {
            process_range(cursors, end);
        }
        process_range(cursors, max_docid);

        m_topk.finalize();
        return m_topk.topk().size();
    }

    std::vector<std::pair<float, uint64_t>> const &topk() const { return m_topk.topk(); }

    template <typename CursorRange>
    void process_range(CursorRange &&cursors, size_t end)
    {
        m_algorithm(cursors, end);
        auto &small_topk = m_algorithm.topk();
        for (const auto &entry : small_topk) {
            m_topk.insert(entry.first, entry.second);
        }
    }

   private:
    uint64_t m_k;
    topk_queue m_topk;
    QueryAlg m_algorithm;
};

} // namespace pisa
