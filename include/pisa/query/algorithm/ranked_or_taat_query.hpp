#pragma once

#include "accumulator/partial_score_accumulator.hpp"
#include "concepts.hpp"
#include "topk_queue.hpp"

namespace pisa {

class ranked_or_taat_query {
  public:
    explicit ranked_or_taat_query(topk_queue& topk) : m_topk(topk) {}

    template <typename CursorRange, typename Acc>
    PISA_REQUIRES(PartialScoreAccumulator<Acc>)
    void operator()(CursorRange&& cursors, uint64_t max_docid, Acc&& accumulator) {
        if (cursors.empty()) {
            return;
        }
        accumulator.reset();

        for (auto&& cursor: cursors) {
            while (cursor.docid() < max_docid) {
                accumulator.accumulate(cursor.docid(), cursor.score());
                cursor.next();
            }
        }
        accumulator.collect(m_topk);
    }

    std::vector<typename topk_queue::entry_type> const& topk() const { return m_topk.topk(); }

  private:
    topk_queue& m_topk;
};

};  // namespace pisa
