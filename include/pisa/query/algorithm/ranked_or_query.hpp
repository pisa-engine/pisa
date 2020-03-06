#pragma once

#include "query/queries.hpp"
#include "topk_queue.hpp"
#include <string>
#include <vector>

namespace pisa {

struct ranked_or_query {
    ranked_or_query(topk_queue& topk) : m_topk(topk) {}

    template <typename CursorRange>
    void operator()(CursorRange&& cursors, uint64_t max_docid)
    {
        using Cursor = typename std::decay_t<CursorRange>::value_type;
        if (cursors.empty())
            return;
        uint64_t cur_doc =
            std::min_element(cursors.begin(), cursors.end(), [](Cursor const& lhs, Cursor const& rhs) {
                return lhs.docs_enum.docid() < rhs.docs_enum.docid();
            })->docs_enum.docid();

        while (cur_doc < max_docid) {
            float score = 0;
            uint64_t next_doc = max_docid;
            for (size_t i = 0; i < cursors.size(); ++i) {
                if (cursors[i].docs_enum.docid() == cur_doc) {
                    score +=
                        cursors[i].scorer(cursors[i].docs_enum.docid(), cursors[i].docs_enum.freq());
                    cursors[i].docs_enum.next();
                }
                if (cursors[i].docs_enum.docid() < next_doc) {
                    next_doc = cursors[i].docs_enum.docid();
                }
            }

            m_topk.insert(score, cur_doc);
            cur_doc = next_doc;
        }
    }

    std::vector<std::pair<float, uint64_t>> const& topk() const { return m_topk.topk(); }

  private:
    topk_queue& m_topk;
};

}  // namespace pisa
