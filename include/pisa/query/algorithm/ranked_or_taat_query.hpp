#pragma once

#include "query/queries.hpp"
#include "scorer/bm25.hpp"
#include "topk_queue.hpp"
#include "util/intrinsics.hpp"

#include "accumulator/simple_accumulator.hpp"

namespace pisa {

template <typename Acc = Simple_Accumulator>
class ranked_or_taat_query {
   public:
    ranked_or_taat_query(uint64_t k, uint64_t max_docid)
        : m_topk(k), m_max_docid(max_docid), m_accumulator(max_docid) {}

    template <typename CursorRange>
    uint64_t operator()(CursorRange &&cursors) {
        using Cursor = typename CursorRange::value_type;
        m_topk.clear();
        if (cursors.empty()) {
            return 0;
        }
        m_accumulator.init();

        for (auto &&cursor : cursors) {
            while (cursor.docs_enum.docid() < m_max_docid) {
                m_accumulator.accumulate(
                    cursor.docs_enum.docid(),
                    cursor.scorer(cursor.docs_enum.docid(), cursor.docs_enum.freq()));
                cursor.docs_enum.next();
            }
        }
        m_accumulator.aggregate(m_topk);
        m_topk.finalize();
        return m_topk.topk().size();
    }

    std::vector<std::pair<float, uint64_t>> const &topk() const { return m_topk.topk(); }

   private:
    topk_queue m_topk;
    uint64_t   m_max_docid;
    Acc        m_accumulator;
};

};  // namespace pisa
