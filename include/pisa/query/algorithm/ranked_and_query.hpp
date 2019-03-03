#pragma once

#include <vector>
#include "query/queries.hpp"

namespace pisa {

struct ranked_and_query {

    typedef bm25 scorer_type;

    ranked_and_query(uint64_t k) : m_topk(k) {}

    template <typename CursorRange>
    uint64_t operator()(CursorRange &&cursors, uint64_t max_docid) {
        using Cursor = typename CursorRange::value_type;
        size_t results = 0;
        m_topk.clear();
        if (cursors.empty())
            return 0;

        std::vector<Cursor *> ordered_cursors;
        ordered_cursors.reserve(cursors.size());
        for (auto &en : cursors) {
            ordered_cursors.push_back(&en);
        }


        // sort by increasing frequency
        std::sort(ordered_cursors.begin(), ordered_cursors.end(), [](Cursor *lhs, Cursor *rhs) {
            return lhs->docs_enum.size() < rhs->docs_enum.size();
        });

        uint64_t candidate = ordered_cursors[0]->docs_enum.docid();
        size_t   i         = 1;
        while (candidate < max_docid) {
            for (; i < ordered_cursors.size(); ++i) {
                ordered_cursors[i]->docs_enum.next_geq(candidate);
                if (ordered_cursors[i]->docs_enum.docid() != candidate) {
                    candidate = ordered_cursors[i]->docs_enum.docid();
                    i         = 0;
                    break;
                }
            }

            if (i == ordered_cursors.size()) {
                float score    = 0;
                for (i = 0; i < ordered_cursors.size(); ++i) {
                    score += ordered_cursors[i]->scorer(ordered_cursors[i]->docs_enum.docid(), ordered_cursors[i]->docs_enum.freq());
                }

                m_topk.insert(score, ordered_cursors[0]->docs_enum.docid());

                results++;
                if (results >= m_topk.size() * 2)
                    break;

                ordered_cursors[0]->docs_enum.next();
                candidate = ordered_cursors[0]->docs_enum.docid();
                i         = 1;
            }
        }

        //    m_topk.finalize();
        return m_topk.topk().size();
    }

    std::vector<std::pair<float, uint64_t>> const &topk() const { return m_topk.topk(); }

    topk_queue &get_topk() { return m_topk; }

   private:
    topk_queue      m_topk;
};

} // namespace pisa