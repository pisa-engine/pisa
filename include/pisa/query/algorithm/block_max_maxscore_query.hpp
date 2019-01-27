#pragma once

#include <vector>
#include "query/queries.hpp"

namespace pisa {

template <typename Index, typename WandType>
struct block_max_maxscore_query {

    typedef bm25 scorer_type;

    block_max_maxscore_query(Index const &index, WandType const &wdata, uint64_t k)
        : m_index(index), m_wdata(&wdata), m_topk(k) {}

    uint64_t operator()(term_id_vec const &terms) {
        m_topk.clear();
        if (terms.empty())
            return 0;

        auto query_term_freqs = query_freqs(terms);

        uint64_t                                        num_docs = m_index.num_docs();
        typedef typename Index::document_enumerator     enum_type;
        typedef typename WandType::wand_data_enumerator wdata_enum;

        struct scored_enum {
            enum_type  docs_enum;
            wdata_enum w;
            float      q_weight;
            float      max_weight;
        };

        std::vector<scored_enum> enums;
        enums.reserve(query_term_freqs.size());

        for (auto term : query_term_freqs) {
            auto list       = m_index[term.first];
            auto w_enum     = m_wdata->getenum(term.first);
            auto q_weight   = scorer_type::query_term_weight(term.second, list.size(), num_docs);
            auto max_weight = q_weight * m_wdata->max_term_weight(term.first);
            enums.push_back(scored_enum{std::move(list), w_enum, q_weight, max_weight});
        }

        std::vector<scored_enum *> ordered_enums;
        ordered_enums.reserve(enums.size());
        for (auto &en : enums) {
            ordered_enums.push_back(&en);
        }

        // sort enumerators by increasing maxscore
        std::sort(
            ordered_enums.begin(), ordered_enums.end(), [](scored_enum *lhs, scored_enum *rhs) {
                return lhs->max_weight < rhs->max_weight;
            });

        std::vector<float> upper_bounds(ordered_enums.size());
        upper_bounds[0] = ordered_enums[0]->max_weight;
        for (size_t i = 1; i < ordered_enums.size(); ++i) {
            upper_bounds[i] = upper_bounds[i - 1] + ordered_enums[i]->max_weight;
        }

        int      non_essential_lists = 0;
        uint64_t cur_doc =
            std::min_element(enums.begin(),
                             enums.end(),
                             [](scored_enum const &lhs, scored_enum const &rhs) {
                                 return lhs.docs_enum.docid() < rhs.docs_enum.docid();
                             })
                ->docs_enum.docid();

        while (non_essential_lists < ordered_enums.size() && cur_doc < m_index.num_docs()) {
            float    score    = 0;
            float    norm_len = m_wdata->norm_len(cur_doc);
            uint64_t next_doc = m_index.num_docs();
            for (size_t i = non_essential_lists; i < ordered_enums.size(); ++i) {
                if (ordered_enums[i]->docs_enum.docid() == cur_doc) {
                    score +=
                        ordered_enums[i]->q_weight *
                        scorer_type::doc_term_weight(ordered_enums[i]->docs_enum.freq(), norm_len);
                    ordered_enums[i]->docs_enum.next();
                }
                if (ordered_enums[i]->docs_enum.docid() < next_doc) {
                    next_doc = ordered_enums[i]->docs_enum.docid();
                }
            }

            double block_upper_bound =
                non_essential_lists > 0 ? upper_bounds[non_essential_lists - 1] : 0;
            for (int i = non_essential_lists - 1; i + 1 > 0; --i) {
                if (ordered_enums[i]->w.docid() < cur_doc) {
                    ordered_enums[i]->w.next_geq(cur_doc);
                }
                block_upper_bound -= ordered_enums[i]->max_weight -
                                     ordered_enums[i]->w.score() * ordered_enums[i]->q_weight;
                if (!m_topk.would_enter(score + block_upper_bound)) {
                    break;
                }
            }
            if (m_topk.would_enter(score + block_upper_bound)) {
                // try to complete evaluation with non-essential lists
                for (size_t i = non_essential_lists - 1; i + 1 > 0; --i) {
                    ordered_enums[i]->docs_enum.next_geq(cur_doc);
                    if (ordered_enums[i]->docs_enum.docid() == cur_doc) {
                        auto s = ordered_enums[i]->q_weight *
                                 scorer_type::doc_term_weight(ordered_enums[i]->docs_enum.freq(),
                                                              norm_len);
                        // score += s;
                        block_upper_bound += s;
                    }
                    block_upper_bound -= ordered_enums[i]->w.score() * ordered_enums[i]->q_weight;

                    if (!m_topk.would_enter(score + block_upper_bound)) {
                        break;
                    }
                }
                score += block_upper_bound;
            }
            if (m_topk.insert(score)) {
                // update non-essential lists
                while (non_essential_lists < ordered_enums.size() &&
                       !m_topk.would_enter(upper_bounds[non_essential_lists])) {
                    non_essential_lists += 1;
                }
            }
            cur_doc = next_doc;
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