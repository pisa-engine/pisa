#pragma once

namespace pisa {
template <typename Index,typename WandType>
struct ranked_or_taat_query {

    typedef bm25 scorer_type;

    ranked_or_taat_query(Index const &index, WandType const &wdata, uint64_t k)
        : m_index(index), m_wdata(&wdata), m_topk(k) {}

    uint64_t operator()(term_id_vec terms) {
        m_topk.clear();
        if (terms.empty())
            return 0;

        auto query_term_freqs = query_freqs(terms);

        uint64_t           num_docs = m_index.num_docs();
        std::vector<float> accumulator(num_docs, 0.0f);
        for (auto term : query_term_freqs) {
            auto list     = m_index[term.first];
            auto q_weight = scorer_type::query_term_weight(term.second, list.size(), num_docs);
            auto cur_doc  = list.docid();
            while (cur_doc < num_docs) {
                float norm_len = m_wdata->norm_len(cur_doc);
                float score    = q_weight * scorer_type::doc_term_weight(list.freq(), norm_len);
                accumulator[cur_doc] += score;
                list.next();
                cur_doc = list.docid();
            }
        }

        for (auto &&v : accumulator) {
            m_topk.insert(v);
        }

        m_topk.finalize();
        return m_topk.topk().size();
    }

    std::vector<std::pair<float, uint64_t>> const &topk() const { return m_topk.topk(); }

   private:
    Index const &   m_index;
    WandType const *m_wdata;
    topk_queue      m_topk;
};

} // namespace pisa