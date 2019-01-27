#pragma once

#include <vector>
#include "query/queries.hpp"

namespace pisa {

template <typename Index, typename WandType>
struct ranked_and_query {

    typedef bm25 scorer_type;

    ranked_and_query(Index const &index, WandType const &wdata, uint64_t k)
        : m_index(index), m_wdata(&wdata), m_topk(k) {}

    uint64_t operator()(term_id_vec terms) {
        size_t results = 0;
        m_topk.clear();
        if (terms.empty())
            return 0;

        auto query_term_freqs = query_freqs(terms);

        uint64_t                                    num_docs = m_index.num_docs();
        typedef typename Index::document_enumerator enum_type;
        struct scored_enum {
            enum_type docs_enum;
            float     q_weight;
        };

        std::vector<scored_enum> enums;
        enums.reserve(query_term_freqs.size());

        for (auto term : query_term_freqs) {
            auto list     = m_index[term.first];
            auto q_weight = scorer_type::query_term_weight(term.second, list.size(), num_docs);
            enums.push_back(scored_enum{std::move(list), q_weight});
        }

        // sort by increasing frequency
        std::sort(enums.begin(), enums.end(), [](scored_enum const &lhs, scored_enum const &rhs) {
            return lhs.docs_enum.size() < rhs.docs_enum.size();
        });

        uint64_t candidate = enums[0].docs_enum.docid();
        size_t   i         = 1;
        while (candidate < m_index.num_docs()) {
            for (; i < enums.size(); ++i) {
                enums[i].docs_enum.next_geq(candidate);
                if (enums[i].docs_enum.docid() != candidate) {
                    candidate = enums[i].docs_enum.docid();
                    i         = 0;
                    break;
                }
            }

            if (i == enums.size()) {
                float norm_len = m_wdata->norm_len(candidate);
                float score    = 0;
                for (i = 0; i < enums.size(); ++i) {
                    score += enums[i].q_weight *
                             scorer_type::doc_term_weight(enums[i].docs_enum.freq(), norm_len);
                }

                m_topk.insert(score, enums[0].docs_enum.docid());

                results++;
                if (results >= m_topk.size() * 2)
                    break;

                enums[0].docs_enum.next();
                candidate = enums[0].docs_enum.docid();
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
};

} // namespace pisa