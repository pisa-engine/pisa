#pragma once

#include <numeric>
#include <vector>

#include "algorithm/numeric.hpp"
#include "query/queries.hpp"

namespace pisa {

template <typename Cursor>
[[nodiscard]] auto score_pivot_function(std::vector<Cursor *> const &ordered_cursors,
                                        topk_queue &top_k)
{
    return [&top_k, &ordered_cursors](auto pivot_id, auto block_upper_bound) {
        float score = 0.f;
        auto pos = for_each(ordered_cursors.begin(),
                            ordered_cursors.end(),
                            while_holds([&](auto const *cursor) {
                                return cursor->docid() == pivot_id and
                                       top_k.would_enter(block_upper_bound);
                            }),
                            [&](auto *cursor) {
                                auto partial_score = cursor->score();
                                score += partial_score;
                                block_upper_bound -= cursor->block_max_score() - partial_score;
                                cursor->next();
                            });
        for_each(pos,
                 ordered_cursors.end(),
                 while_holds([&](auto const *cursor) { return cursor->docid() == pivot_id; }),
                 [&](auto *cursor) { cursor->next(); });
        return score;
    };
}

template <typename Cursor>
void move_first_leq_pivot(gsl::span<Cursor> ordered_cursors, int pivot, uint32_t pivot_id)
{
    auto next_list = pivot;
    for (; ordered_cursors[next_list]->docid() == pivot_id; --next_list) {}
    ordered_cursors[next_list]->next_geq(pivot_id);

    for (size_t i = next_list + 1; i < ordered_cursors.size(); ++i) {
        if (ordered_cursors[i]->docid() <= ordered_cursors[i - 1]->docid()) {
            std::swap(ordered_cursors[i], ordered_cursors[i - 1]);
        }
        else {
            break;
        }
    }
}

template <typename Index, typename WandType>
struct block_max_wand_query {
    typedef bm25 scorer_type;

    block_max_wand_query(Index const &index, WandType const &wdata, uint64_t k)
        : m_index(index), m_wdata(&wdata), m_topk(k) {}

    template <typename Block_Max_Scored_Range>
    auto operator()(gsl::span<Block_Max_Scored_Range> posting_ranges) -> int64_t
    {
        using cursor_type = typename Block_Max_Scored_Range::cursor_type;
        m_topk.clear();
        if (posting_ranges.empty()) {
            return 0;
        }

        std::vector<cursor_type> cursors = query::open_cursors(posting_ranges);
        std::vector<cursor_type *> ordered_cursors =
            query::map_to_pointers(gsl::make_span(cursors));

        auto sort_cursors = [&]() {
            std::sort(ordered_cursors.begin(), ordered_cursors.end(), [](auto *lhs, auto *rhs) {
                return lhs->docid() < rhs->docid();
            });
        };

        auto find_pivot = [&](size_t &pivot) {
            float upper_bound = 0.f;
            bool found_pivot = false;
            for (pivot = 0u; pivot < ordered_cursors.size(); ++pivot) {
                if (ordered_cursors[pivot]->docid() == pisa::cursor::document_bound) {
                    break;
                }
                upper_bound += ordered_cursors[pivot]->max_score();
                if (m_topk.would_enter(upper_bound)) {
                    auto pivot_id = ordered_cursors[pivot]->docid();
                    found_pivot = true;
                    for (; pivot + 1 < ordered_cursors.size() &&
                           ordered_cursors[pivot + 1]->docid() == pivot_id;
                         ++pivot) {}
                    break;
                }
            }
            return found_pivot;
        };
        auto score_pivot = score_pivot_function(ordered_cursors, m_topk);

        sort_cursors();
        size_t pivot;
        while (find_pivot(pivot)) {
            auto pivot_id = ordered_cursors[pivot]->docid();
            float block_upper_bound = pisa::transform_reduce(
                ordered_cursors.begin(),
                std::next(ordered_cursors.begin(), pivot + 1),
                0.f,
                std::plus<>(),
                [&](auto *cursor) { return cursor->block_max_score(pivot_id); });

            if (m_topk.would_enter(block_upper_bound)) {
                if (pivot_id == ordered_cursors[0]->docid()) {
                    auto score = score_pivot(pivot_id, block_upper_bound);
                    m_topk.insert(score, pivot_id);
                    sort_cursors();
                }
                else {
                    move_first_leq_pivot(gsl::make_span(ordered_cursors), pivot, pivot_id);
                }
            }
            else {
                auto next_list = std::max_element(
                    ordered_cursors.begin(),
                    std::next(ordered_cursors.begin(), pivot + 1),
                    [](auto *lhs, auto *rhs) { return lhs->term_weight() < rhs->term_weight(); });
                auto next_lookup_id = pisa::cursor::document_bound;
                if (pivot + 1 < ordered_cursors.size()) {
                    next_lookup_id = ordered_cursors[pivot + 1]->docid();
                }

                for (size_t i = 0; i <= pivot; ++i) {
                    if (ordered_cursors[i]->block_docid() < next_lookup_id) {
                        next_lookup_id =
                            std::min(ordered_cursors[i]->block_docid(), next_lookup_id);
                    }
                }

                auto next = next_lookup_id + 1;
                if (pivot + 1 < ordered_cursors.size()) {
                    if (next > ordered_cursors[pivot + 1]->docid()) {
                        next = ordered_cursors[pivot + 1]->docid();
                    }
                }

                if (next <= ordered_cursors[pivot]->docid()) {
                    next = ordered_cursors[pivot]->docid() + 1;
                }

                (*next_list)->next_geq(next);
                bubble_down(next_list, ordered_cursors.end());
            }
        }
        m_topk.finalize();
        return m_topk.topk().size();
    }

    uint64_t operator()(term_id_vec const &terms) {
        m_topk.clear();

        if (terms.empty())
            return 0;
        auto                                            query_term_freqs = query_freqs(terms);
        uint64_t                                        num_docs         = m_index.num_docs();
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
            auto list     = m_index[term.first];
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
    Index const &   m_index;
    WandType const *m_wdata;
    topk_queue      m_topk;
};

} // namespace pisa
