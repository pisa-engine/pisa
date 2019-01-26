#pragma once

#include <vector>

#include "scorer/bm25.hpp"
#include "topk_queue.hpp"
#include "query/queries.hpp"

namespace pisa {

template <typename Index, typename WandType>
struct wand_query {

    typedef bm25 scorer_type;

    wand_query(Index const &index, WandType const &wdata, uint64_t k)
        : m_index(index), m_wdata(&wdata), m_topk(k) {}

    uint64_t operator()(term_id_vec const &terms) {
        m_topk.clear();
        if (terms.empty())
            return 0;

        auto query_term_freqs = query_freqs(terms);

        uint64_t                                    num_docs = m_index.num_docs();
        typedef typename Index::document_enumerator enum_type;
        struct scored_enum {
            enum_type docs_enum;
            float     q_weight;
            float     max_weight;
        };

        std::vector<scored_enum> enums;
        enums.reserve(query_term_freqs.size());

        for (auto term : query_term_freqs) {
            auto list     = m_index[term.first];
            auto q_weight = scorer_type::query_term_weight(term.second, list.size(), num_docs);

            auto max_weight = q_weight * m_wdata->max_term_weight(term.first);
            enums.push_back(scored_enum{std::move(list), q_weight, max_weight});
        }

        std::vector<scored_enum *> ordered_enums;
        ordered_enums.reserve(enums.size());
        for (auto &en : enums) {
            ordered_enums.push_back(&en);
        }

        auto sort_enums = [&]() {
            // sort enumerators by increasing docid
            std::sort(
                ordered_enums.begin(), ordered_enums.end(), [](scored_enum *lhs, scored_enum *rhs) {
                    return lhs->docs_enum.docid() < rhs->docs_enum.docid();
                });
        };

        sort_enums();
        while (true) {
            // find pivot
            float  upper_bound = 0;
            size_t pivot;
            bool   found_pivot = false;
            for (pivot = 0; pivot < ordered_enums.size(); ++pivot) {
                if (ordered_enums[pivot]->docs_enum.docid() == num_docs) {
                    break;
                }
                upper_bound += ordered_enums[pivot]->max_weight;
                if (m_topk.would_enter(upper_bound)) {
                    found_pivot = true;
                    break;
                }
            }

            // no pivot found, we can stop the search
            if (!found_pivot) {
                break;
            }

            // check if pivot is a possible match
            uint64_t pivot_id = ordered_enums[pivot]->docs_enum.docid();
            if (pivot_id == ordered_enums[0]->docs_enum.docid()) {
                float score    = 0;
                float norm_len = m_wdata->norm_len(pivot_id);
                for (scored_enum *en : ordered_enums) {
                    if (en->docs_enum.docid() != pivot_id) {
                        break;
                    }
                    score +=
                        en->q_weight * scorer_type::doc_term_weight(en->docs_enum.freq(), norm_len);
                    en->docs_enum.next();
                }

                m_topk.insert(score, pivot_id);
                // resort by docid
                sort_enums();
            } else {
                // no match, move farthest list up to the pivot
                uint64_t next_list = pivot;
                for (; ordered_enums[next_list]->docs_enum.docid() == pivot_id; --next_list)
                    ;
                ordered_enums[next_list]->docs_enum.next_geq(pivot_id);
                // bubble down the advanced list
                for (size_t i = next_list + 1; i < ordered_enums.size(); ++i) {
                    if (ordered_enums[i]->docs_enum.docid() <
                        ordered_enums[i - 1]->docs_enum.docid()) {
                        std::swap(ordered_enums[i], ordered_enums[i - 1]);
                    } else {
                        break;
                    }
                }
            }
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