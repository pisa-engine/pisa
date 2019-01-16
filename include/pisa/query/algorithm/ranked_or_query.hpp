#pragma once

namespace pisa {

template <typename WandType>
struct ranked_or_query {

    typedef bm25 scorer_type;

    ranked_or_query(WandType const &wdata, uint64_t k) : m_wdata(&wdata), m_topk(k) {}

    template <typename Index>
    uint64_t operator()(Index const &index, term_id_vec terms) {
        m_topk.clear();
        if (terms.empty())
            return 0;

        auto query_term_freqs = query_freqs(terms);

        uint64_t                                    num_docs = index.num_docs();
        typedef typename Index::document_enumerator enum_type;
        struct scored_enum {
            enum_type docs_enum;
            float     q_weight;
        };

        std::vector<scored_enum> enums;
        enums.reserve(query_term_freqs.size());

        for (auto term : query_term_freqs) {
            auto list     = index[term.first];
            auto q_weight = scorer_type::query_term_weight(term.second, list.size(), num_docs);
            enums.push_back(scored_enum{std::move(list), q_weight});
        }

        uint64_t cur_doc =
            std::min_element(enums.begin(),
                             enums.end(),
                             [](scored_enum const &lhs, scored_enum const &rhs) {
                                 return lhs.docs_enum.docid() < rhs.docs_enum.docid();
                             })
                ->docs_enum.docid();

        while (cur_doc < index.num_docs()) {
            float    score    = 0;
            float    norm_len = m_wdata->norm_len(cur_doc);
            uint64_t next_doc = index.num_docs();
            for (size_t i = 0; i < enums.size(); ++i) {
                if (enums[i].docs_enum.docid() == cur_doc) {
                    score += enums[i].q_weight *
                             scorer_type::doc_term_weight(enums[i].docs_enum.freq(), norm_len);
                    enums[i].docs_enum.next();
                }
                if (enums[i].docs_enum.docid() < next_doc) {
                    next_doc = enums[i].docs_enum.docid();
                }
            }

            m_topk.insert(score);
            cur_doc = next_doc;
        }

        m_topk.finalize();
        return m_topk.topk().size();
    }

    std::vector<std::pair<float, uint64_t>> const &topk() const { return m_topk.topk(); }

   private:
    WandType const *m_wdata;
    topk_queue      m_topk;
};

} // namespace pisa