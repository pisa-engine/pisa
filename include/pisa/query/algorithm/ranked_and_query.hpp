#pragma once

#include <vector>
#include "query/queries.hpp"

namespace pisa {

template <typename Index, typename WandType>
struct ranked_and_query {

    typedef bm25 scorer_type;

    ranked_and_query(Index const &index, WandType const &wdata, uint64_t k, uint64_t max_docid)
        : m_index(index), m_wdata(&wdata), m_topk(k), m_max_docid(max_docid) {}

    template <typename Cursor>
    uint64_t operator()(std::vector<Cursor> &&cursors) {
        size_t results = 0;
        m_topk.clear();
        if (cursors.empty())
            return 0;

        // sort by increasing frequency
        std::sort(cursors.begin(), cursors.end(), [](Cursor const &lhs, Cursor const &rhs) {
            return lhs.docs_enum.size() < rhs.docs_enum.size();
        });

        uint64_t candidate = cursors[0].docs_enum.docid();
        size_t   i         = 1;
        while (candidate < m_max_docid) {
            for (; i < cursors.size(); ++i) {
                cursors[i].docs_enum.next_geq(candidate);
                if (cursors[i].docs_enum.docid() != candidate) {
                    candidate = cursors[i].docs_enum.docid();
                    i         = 0;
                    break;
                }
            }

            if (i == cursors.size()) {
                float norm_len = m_wdata->norm_len(candidate);
                float score    = 0;
                for (i = 0; i < cursors.size(); ++i) {
                    score += cursors[i].q_weight *
                             scorer_type::doc_term_weight(cursors[i].docs_enum.freq(), norm_len);
                }

                m_topk.insert(score, cursors[0].docs_enum.docid());

                results++;
                if (results >= m_topk.size() * 2)
                    break;

                cursors[0].docs_enum.next();
                candidate = cursors[0].docs_enum.docid();
                i         = 1;
            }
        }

        //    m_topk.finalize();
        return m_topk.topk().size();
    }

    std::vector<std::pair<float, uint64_t>> const &topk() const { return m_topk.topk(); }

    topk_queue &get_topk() { return m_topk; }

   private:
    Index const &   m_index;
    WandType const *m_wdata;
    topk_queue      m_topk;
    uint64_t m_max_docid;
};

} // namespace pisa