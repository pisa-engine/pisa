#pragma once

#include "query/queries.hpp"
#include "topk_queue.hpp"

namespace pisa {

template <typename QueryAlg>
struct range_taat_query {
    explicit range_taat_query(topk_queue& topk) : m_topk(topk) {}

    template <typename CursorRange, typename Acc>
    void operator()(CursorRange&& cursors, uint64_t max_docid, size_t range_size, Acc&& accumulator)
    {
        if (cursors.empty()) {
            return;
        }

        accumulator.init();

        for (size_t end = range_size; end + range_size < max_docid; end += range_size) {
            process_range(cursors, end, accumulator);
        }
        process_range(cursors, max_docid, accumulator);
    }

    std::vector<typename topk_queue::entry_type> const& topk() const { return m_topk.topk(); }

    template <typename CursorRange, typename Acc>
    void process_range(CursorRange&& cursors, size_t end, Acc&& accumulator)
    {
        QueryAlg query_alg(m_topk);
        query_alg(cursors, end, accumulator);
    }

  private:
    topk_queue& m_topk;
};

}  // namespace pisa
