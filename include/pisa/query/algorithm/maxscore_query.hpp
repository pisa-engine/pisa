#pragma once

#include <algorithm>
#include <numeric>
#include <vector>

#include "query/queries.hpp"

namespace pisa {

template <typename Index, typename WandType>
struct maxscore_query {

    typedef bm25 scorer_type;

    maxscore_query(Index const &index, WandType const &wdata, uint64_t k)
        : m_index(index), m_wdata(&wdata), m_topk(k)
    {}

    template <typename Max_Scored_Range>
    auto operator()(gsl::span<Max_Scored_Range> posting_ranges) -> int64_t
    {
        using cursor_type = typename Max_Scored_Range::cursor_type;
        m_topk.clear();
        if (posting_ranges.empty()) {
            return 0;
        }

        std::vector<cursor_type> cursors = query::open_cursors(posting_ranges);
        std::sort(cursors.begin(), cursors.end(), [](auto const &lhs, auto const &rhs) {
            return lhs.max_score() < rhs.max_score();
        });

        std::vector<float> upper_bounds(cursors.size());
        std::transform(cursors.begin(),
                       cursors.end(),
                       upper_bounds.begin(),
                       [](auto const &cursor) { return cursor.max_score(); });
        std::partial_sum(upper_bounds.begin(), upper_bounds.end(), upper_bounds.begin());

        auto first_essential = cursors.begin();
        auto first_essential_bound = upper_bounds.begin();
        auto current_doc = std::min_element(cursors.begin(),
                                            cursors.end(),
                                            [](auto const &lhs, auto const &rhs) {
                                                return lhs.docid() < rhs.docid();
                                            })
                               ->docid();

        while (first_essential != cursors.end() && current_doc < pisa::cursor::document_bound) {
            auto score = 0.f;
            auto next_doc = pisa::cursor::document_bound;
            std::for_each(first_essential, cursors.end(), [&](auto &cursor) {
                if (cursor.docid() == current_doc) {
                    score += cursor.score();
                    cursor.next();
                }
                if (auto id = cursor.docid(); id < next_doc) {
                    next_doc = id;
                }
            });

            for_each_pair(std::reverse_iterator(first_essential),
                          cursors.rend(),
                          std::reverse_iterator(first_essential_bound),
                          upper_bounds.rend(),
                          while_holds([&](auto &cursor, auto const &bound) {
                              return m_topk.would_enter(score + bound);
                          }),
                          [&](auto &cursor, auto const & /*bound*/) {
                              cursor.next_geq(current_doc);
                              if (cursor.docid() == current_doc) {
                                  score += cursor.score();
                              }
                          });

            if (m_topk.insert(score, current_doc)) {
                while (first_essential < cursors.end() &&
                       not m_topk.would_enter(*first_essential_bound)) {
                    ++first_essential;
                    ++first_essential_bound;
                }
            }

            current_doc = next_doc;
        }

        m_topk.finalize();
        return m_topk.topk().size();
    }

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
            auto list       = m_index[term.first];
            auto q_weight   = scorer_type::query_term_weight(term.second, list.size(), num_docs);
            auto max_weight = q_weight * m_wdata->max_term_weight(term.first);
            enums.push_back(scored_enum{std::move(list), q_weight, max_weight});
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

        uint64_t non_essential_lists = 0;
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

            // try to complete evaluation with non-essential lists
            for (size_t i = non_essential_lists - 1; i + 1 > 0; --i) {
                if (!m_topk.would_enter(score + upper_bounds[i])) {
                    break;
                }
                ordered_enums[i]->docs_enum.next_geq(cur_doc);
                if (ordered_enums[i]->docs_enum.docid() == cur_doc) {
                    score +=
                        ordered_enums[i]->q_weight *
                        scorer_type::doc_term_weight(ordered_enums[i]->docs_enum.freq(), norm_len);
                }
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
