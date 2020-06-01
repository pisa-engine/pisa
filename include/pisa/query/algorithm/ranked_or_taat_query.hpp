#pragma once

#include "query/queries.hpp"
#include "topk_queue.hpp"
#include "util/intrinsics.hpp"

#include "accumulator/simple_accumulator.hpp"

#include "topk_queue.hpp"

namespace pisa {

class ranked_or_taat_query {
  public:
    explicit ranked_or_taat_query(topk_queue& topk) : m_topk(topk) {}

    template <typename CursorRange, typename Acc>
    void operator()(CursorRange&& cursors, uint64_t max_docid, Acc&& accumulator)
    {
        using Cursor = typename std::decay_t<CursorRange>::value_type;
        if (cursors.empty()) {
            return;
        }
        accumulator.init();

        for (auto&& cursor: cursors) {
            while (cursor.docid() < max_docid) {
                accumulator.accumulate(cursor.docid(), cursor.score());
                cursor.next();
            }
        }
        accumulator.aggregate(m_topk);
    }

    std::vector<std::pair<float, uint64_t>> const& topk() const { return m_topk.topk(); }

  private:
    topk_queue& m_topk;
};

};  // namespace pisa
