#pragma once

#include "query/queries.hpp"
#include "topk_queue.hpp"
#include "util/intrinsics.hpp"

#include "accumulator/simple_accumulator.hpp"

namespace pisa {

class ranked_or_taat_query {
   public:
    ranked_or_taat_query(uint64_t k)
        : m_topk(k) {}

    template <typename CursorRange, typename Acc>
    uint64_t operator()(CursorRange &&cursors, uint64_t max_docid, Acc accumulator) {
        using Cursor = typename std::decay_t<CursorRange>::value_type;
        m_topk.clear();
        if (cursors.empty()) {
            return 0;
        }
        accumulator.init();

        for (auto &&cursor : cursors) {
            while (cursor.docs_enum.docid() < max_docid) {
                accumulator.accumulate(
                    cursor.docs_enum.docid(),
                    cursor.scorer(cursor.docs_enum.docid(), cursor.docs_enum.freq()));
                cursor.docs_enum.next();
            }
        }
        accumulator.aggregate(m_topk);
        m_topk.finalize();
        return m_topk.topk().size();
    }

    std::vector<std::pair<float, uint64_t>> const &topk() const { return m_topk.topk(); }

   private:
    topk_queue m_topk;
};

};  // namespace pisa
