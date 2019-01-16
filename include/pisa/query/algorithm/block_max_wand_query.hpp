#pragma once

namespace pisa {

template <typename WandType>
struct block_max_wand_query {
    typedef bm25 scorer_type;

    block_max_wand_query(WandType const &wdata, uint64_t k) : m_wdata(&wdata), m_topk(k) {}

    template <typename Index>
    uint64_t operator()(Index const &index, term_id_vec const &terms) {
        m_topk.clear();

        if (terms.empty())
            return 0;
        auto                                            query_term_freqs = query_freqs(terms);
        uint64_t                                        num_docs         = index.num_docs();
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
            auto list     = index[term.first];
            auto w_enum   = m_wdata->getenum(term.first);
            auto q_weight = scorer_type::query_term_weight(term.second, list.size(), num_docs);

            float max_weight = q_weight * m_wdata->max_term_weight(term.first);
            enums.push_back(scored_enum{std::move(list), w_enum, q_weight, max_weight});
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
            float    upper_bound = 0.f;
            size_t   pivot;
            bool     found_pivot = false;
            uint64_t pivot_id    = num_docs;

            for (pivot = 0; pivot < ordered_enums.size(); ++pivot) {
                if (ordered_enums[pivot]->docs_enum.docid() == num_docs) {
                    break;
                }

                upper_bound += ordered_enums[pivot]->max_weight;
                if (m_topk.would_enter(upper_bound)) {
                    found_pivot = true;
                    pivot_id    = ordered_enums[pivot]->docs_enum.docid();
                    for (; pivot + 1 < ordered_enums.size() &&
                           ordered_enums[pivot + 1]->docs_enum.docid() == pivot_id;
                         ++pivot)
                        ;
                    break;
                }
            }

            // no pivot found, we can stop the search
            if (!found_pivot) {
                break;
            }

            double block_upper_bound = 0;

            for (size_t i = 0; i < pivot + 1; ++i) {
                if (ordered_enums[i]->w.docid() < pivot_id) {
                    ordered_enums[i]->w.next_geq(pivot_id);
                }

                block_upper_bound += ordered_enums[i]->w.score() * ordered_enums[i]->q_weight;
            }

            if (m_topk.would_enter(block_upper_bound)) {

                // check if pivot is a possible match
                if (pivot_id == ordered_enums[0]->docs_enum.docid()) {
                    float score    = 0;
                    float norm_len = m_wdata->norm_len(pivot_id);

                    for (scored_enum *en : ordered_enums) {
                        if (en->docs_enum.docid() != pivot_id) {
                            break;
                        }
                        float part_score = en->q_weight * scorer_type::doc_term_weight(
                                                              en->docs_enum.freq(), norm_len);
                        score += part_score;
                        block_upper_bound -= en->w.score() * en->q_weight - part_score;
                        if (!m_topk.would_enter(block_upper_bound)) {
                            break;
                        }
                    }
                    for (scored_enum *en : ordered_enums) {
                        if (en->docs_enum.docid() != pivot_id) {
                            break;
                        }
                        en->docs_enum.next();
                    }

                    m_topk.insert(score, pivot_id);
                    // resort by docid
                    sort_enums();

                } else {

                    uint64_t next_list = pivot;
                    for (; ordered_enums[next_list]->docs_enum.docid() == pivot_id; --next_list)
                        ;
                    ordered_enums[next_list]->docs_enum.next_geq(pivot_id);

                    // bubble down the advanced list
                    for (size_t i = next_list + 1; i < ordered_enums.size(); ++i) {
                        if (ordered_enums[i]->docs_enum.docid() <=
                            ordered_enums[i - 1]->docs_enum.docid()) {
                            std::swap(ordered_enums[i], ordered_enums[i - 1]);
                        } else {
                            break;
                        }
                    }
                }

            } else {

                uint64_t next;
                uint64_t next_list = pivot;

                float q_weight = ordered_enums[next_list]->q_weight;

                for (uint64_t i = 0; i < pivot; i++) {
                    if (ordered_enums[i]->q_weight > q_weight) {
                        next_list = i;
                        q_weight  = ordered_enums[i]->q_weight;
                    }
                }

                // TO BE FIXED (change with num_docs())
                uint64_t next_jump = uint64_t(-2);

                if (pivot + 1 < ordered_enums.size()) {
                    next_jump = ordered_enums[pivot + 1]->docs_enum.docid();
                }

                for (size_t i = 0; i <= pivot; ++i) {
                    if (ordered_enums[i]->w.docid() < next_jump)
                        next_jump = std::min(ordered_enums[i]->w.docid(), next_jump);
                }

                next = next_jump + 1;
                if (pivot + 1 < ordered_enums.size()) {
                    if (next > ordered_enums[pivot + 1]->docs_enum.docid()) {
                        next = ordered_enums[pivot + 1]->docs_enum.docid();
                    }
                }

                if (next <= ordered_enums[pivot]->docs_enum.docid()) {
                    next = ordered_enums[pivot]->docs_enum.docid() + 1;
                }

                ordered_enums[next_list]->docs_enum.next_geq(next);

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

    void clear_topk() { m_topk.clear(); }

    topk_queue const &get_topk() const { return m_topk; }

   private:
    WandType const *m_wdata;
    topk_queue      m_topk;
};

} // namespace pisa