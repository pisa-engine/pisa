#pragma once

#include "query/queries.hpp"
#include "topk_queue.hpp"
#include "wand_data_range.hpp"
#include "util/do_not_optimize_away.hpp"

namespace pisa {

template <typename QueryAlg>
struct range_query {
    explicit range_query(topk_queue& topk) : m_topk(topk) {}

    template <typename CursorRange>
    void operator()(CursorRange&& cursors, uint64_t max_docid, size_t range_size,
                    bit_vector const&live_blocks,
                    std::vector<std::vector<uint16_t>> const& scores)
    {
        if (cursors.empty()) {
            return;
        }
        bit_vector::unary_enumerator en(live_blocks, 0);
        size_t i = en.next(), end = (i+1) * range_size;
        for (; i< live_blocks.size() and end < max_docid; i = en.next(), end = (i+1) * range_size) {
                auto min_docid = end - range_size;
                size_t t = 0;
                for (auto&& c: cursors) {
                    // c.docs_enum.next_geq(min_docid);
                    c.max_score(c.scores(i));
                    t += 1;
                }
                process_range(cursors, end);
        }
        // process_range(cursors, max_docid);
    }

    std::vector<std::pair<float, uint64_t>> const& topk() const { return m_topk.topk(); }

    template <typename CursorRange>
    void process_range(CursorRange&& cursors, size_t end)
    {
        QueryAlg query_alg(m_topk);
        query_alg(cursors, end);
    }

  private:
    topk_queue& m_topk;
};

}  // namespace pisa
