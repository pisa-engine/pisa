#pragma once

#include "query/queries.hpp"
#include "topk_queue.hpp"
#include "util/do_not_optimize_away.hpp"
#include "wand_data_range.hpp"

namespace pisa {

template <typename QueryAlg>
struct range_query {
    explicit range_query(topk_queue& topk) : m_topk(topk) {}

    template <typename CursorRange>
    void operator()(
        CursorRange&& cursors, uint64_t max_docid, size_t range_size, bit_vector const& live_blocks)
    {
        if (cursors.empty()) {
            return;
        }
        bit_vector::unary_enumerator en(live_blocks, 0);
        uint64_t i = en.next(), end = (i + 1) * range_size;
        for (; i < live_blocks.size(); i = en.next(), end = (i + 1) * range_size) {
            auto min_docid = end - range_size;
            size_t t = 0;
            for (auto&& c: cursors) {
                c.max_score(c.scores(i));
                t += 1;
            }
            process_range(cursors, min_docid, std::min(end, max_docid));
        }
    }

    std::vector<std::pair<float, uint64_t>> const& topk() const { return m_topk.topk(); }

    template <typename CursorRange>
    void process_range(CursorRange&& cursors, size_t begin, size_t end)
    {
        QueryAlg query_alg(m_topk);
        query_alg(cursors, end, begin);
    }

  private:
    topk_queue& m_topk;
};

}  // namespace pisa
