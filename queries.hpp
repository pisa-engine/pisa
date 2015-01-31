#pragma once

#include <iostream>
#include <sstream>

#include "index_types.hpp"
#include "wand_data.hpp"
#include "util.hpp"

namespace ds2i {

    typedef uint32_t term_id_type;
    typedef std::vector<term_id_type> term_id_vec;

    bool read_query(term_id_vec& ret, std::istream& is = std::cin)
    {
        ret.clear();
        std::string line;
        if (!std::getline(is, line)) return false;
        std::istringstream iline(line);
        term_id_type term_id;
        while (iline >> term_id) {
            ret.push_back(term_id);
        }

        return true;
    }

    void remove_duplicate_terms(term_id_vec& terms)
    {
        std::sort(terms.begin(), terms.end());
        terms.erase(std::unique(terms.begin(), terms.end()), terms.end());
    }

    template <bool with_freqs>
    struct and_query {

        template <typename Index>
        uint64_t operator()(Index const& index, term_id_vec terms) const
        {
            if (terms.empty()) return 0;
            remove_duplicate_terms(terms);

            typedef typename Index::document_enumerator enum_type;
            std::vector<enum_type> enums;
            enums.reserve(terms.size());

            for (auto term: terms) {
                enums.push_back(index[term]);
            }

            // sort by increasing frequency
            std::sort(enums.begin(), enums.end(),
                      [](enum_type const& lhs, enum_type const& rhs) {
                          return lhs.size() < rhs.size();
                      });

            uint64_t results = 0;
            uint64_t candidate = enums[0].docid();
            size_t i = 1;
            while (candidate < index.num_docs()) {
                for (; i < enums.size(); ++i) {
                    enums[i].next_geq(candidate);
                    if (enums[i].docid() != candidate) {
                        candidate = enums[i].docid();
                        i = 0;
                        break;
                    }
                }

                if (i == enums.size()) {
                    results += 1;
                    if (with_freqs) {
                        for (i = 0; i < enums.size(); ++i) {
                            do_not_optimize_away(enums[i].freq());
                        }
                    }
                    enums[0].next();
                    candidate = enums[0].docid();
                    i = 1;
                }
            }

            return results;
        }
    };

    template <bool with_freqs>
    struct or_query {

        template <typename Index>
        uint64_t operator()(Index const& index, term_id_vec terms) const
        {
            if (terms.empty()) return 0;
            remove_duplicate_terms(terms);

            typedef typename Index::document_enumerator enum_type;
            std::vector<enum_type> enums;
            enums.reserve(terms.size());

            for (auto term: terms) {
                enums.push_back(index[term]);
            }

            uint64_t results = 0;
            uint64_t cur_doc = std::min_element(enums.begin(), enums.end(),
                                                [](enum_type const& lhs, enum_type const& rhs) {
                                                    return lhs.docid() < rhs.docid();
                                                })->docid();

            while (cur_doc < index.num_docs()) {
                results += 1;
                uint64_t next_doc = index.num_docs();
                for (size_t i = 0; i < enums.size(); ++i) {
                    if (enums[i].docid() == cur_doc) {
                        if (with_freqs) {
                            do_not_optimize_away(enums[i].freq());
                        }
                        enums[i].next();
                    }
                    if (enums[i].docid() < next_doc) {
                        next_doc = enums[i].docid();
                    }
                }

                cur_doc = next_doc;
            }

            return results;
        }
    };

    typedef std::pair<uint64_t, uint64_t> term_freq_pair;
    typedef std::vector<term_freq_pair> term_freq_vec;

    term_freq_vec query_freqs(term_id_vec terms)
    {
        term_freq_vec query_term_freqs;
        std::sort(terms.begin(), terms.end());
        // count query term frequencies
        for (size_t i = 0; i < terms.size(); ++i) {
            if (i == 0 || terms[i] != terms[i - 1]) {
                query_term_freqs.emplace_back(terms[i], 1);
            } else {
                query_term_freqs.back().second += 1;
            }
        }

        return query_term_freqs;
    }

    struct topk_queue {
        topk_queue(uint64_t k)
            : m_k(k)
        {}

        bool insert(float score)
        {
            if (m_q.size() < m_k) {
                m_q.push_back(score);
                std::push_heap(m_q.begin(), m_q.end(), std::greater<float>());
                return true;
            } else {
                if (score > m_q.front()) {
                    std::pop_heap(m_q.begin(), m_q.end(), std::greater<float>());
                    m_q.back() = score;
                    std::push_heap(m_q.begin(), m_q.end(), std::greater<float>());
                    return true;
                }
            }
            return false;
        }

        bool would_enter(float score) const
        {
            return m_q.size() < m_k || score > m_q.front();
        }

        void finalize()
        {
            std::sort_heap(m_q.begin(), m_q.end(), std::greater<float>());
        }

        std::vector<float> const& topk() const
        {
            return m_q;
        }

        void clear()
        {
            m_q.clear();
        }

    private:
        uint64_t m_k;
        std::vector<float> m_q;
    };


    struct wand_query {

        typedef bm25 scorer_type;

        wand_query(wand_data<scorer_type> const& wdata, uint64_t k)
            : m_wdata(&wdata)
            , m_topk(k)
        {}

        template <typename Index>
        uint64_t operator()(Index const& index, term_id_vec const& terms)
        {
            m_topk.clear();
            if (terms.empty()) return 0;

            auto query_term_freqs = query_freqs(terms);

            uint64_t num_docs = index.num_docs();
            typedef typename Index::document_enumerator enum_type;
            struct scored_enum {
                enum_type docs_enum;
                float q_weight;
                float max_weight;
            };

            std::vector<scored_enum> enums;
            enums.reserve(query_term_freqs.size());

            for (auto term: query_term_freqs) {
                auto list = index[term.first];
                auto q_weight = scorer_type::query_term_weight
                    (term.second, list.size(), num_docs);
                auto max_weight = q_weight * m_wdata->max_term_weight(term.first);
                enums.push_back(scored_enum {std::move(list), q_weight, max_weight});
            }

            std::vector<scored_enum*> ordered_enums;
            ordered_enums.reserve(enums.size());
            for (auto& en: enums) {
                ordered_enums.push_back(&en);
            }

            auto sort_enums = [&]() {
                // sort enumerators by increasing docid
                std::sort(ordered_enums.begin(), ordered_enums.end(),
                          [](scored_enum* lhs, scored_enum* rhs) {
                              return lhs->docs_enum.docid() < rhs->docs_enum.docid();
                          });
            };

            sort_enums();
            while (true) {
                // find pivot
                float upper_bound = 0;
                size_t pivot;
                bool found_pivot = false;
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
                    float score = 0;
                    float norm_len = m_wdata->norm_len(pivot_id);
                    for (scored_enum* en: ordered_enums) {
                        if (en->docs_enum.docid() != pivot_id) {
                            break;
                        }
                        score += en->q_weight * scorer_type::doc_term_weight
                            (en->docs_enum.freq(), norm_len);
                        en->docs_enum.next();
                    }

                    m_topk.insert(score);
                    // resort by docid
                    sort_enums();
                } else {
                    // no match, move farthest list up to the pivot
                    uint64_t next_list = pivot;
                    for (; ordered_enums[next_list]->docs_enum.docid() == pivot_id;
                         --next_list);
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

        std::vector<float> const& topk() const
        {
            return m_topk.topk();
        }

    private:
        wand_data<scorer_type> const* m_wdata;
        topk_queue m_topk;
    };


    struct ranked_and_query {

        typedef bm25 scorer_type;

        ranked_and_query(wand_data<scorer_type> const& wdata, uint64_t k)
            : m_wdata(&wdata)
            , m_topk(k)
        {}

        template <typename Index>
        uint64_t operator()(Index const& index, term_id_vec terms)
        {
            m_topk.clear();
            if (terms.empty()) return 0;

            auto query_term_freqs = query_freqs(terms);

            uint64_t num_docs = index.num_docs();
            typedef typename Index::document_enumerator enum_type;
            struct scored_enum {
                enum_type docs_enum;
                float q_weight;
            };

            std::vector<scored_enum> enums;
            enums.reserve(query_term_freqs.size());

            for (auto term: query_term_freqs) {
                auto list = index[term.first];
                auto q_weight = scorer_type::query_term_weight
                    (term.second, list.size(), num_docs);
                enums.push_back(scored_enum {std::move(list), q_weight});
            }

            // sort by increasing frequency
            std::sort(enums.begin(), enums.end(),
                      [](scored_enum const& lhs, scored_enum const& rhs) {
                          return lhs.docs_enum.size() < rhs.docs_enum.size();
                      });

            uint64_t candidate = enums[0].docs_enum.docid();
            size_t i = 1;
            while (candidate < index.num_docs()) {
                for (; i < enums.size(); ++i) {
                    enums[i].docs_enum.next_geq(candidate);
                    if (enums[i].docs_enum.docid() != candidate) {
                        candidate = enums[i].docs_enum.docid();
                        i = 0;
                        break;
                    }
                }

                if (i == enums.size()) {
                    float norm_len = m_wdata->norm_len(candidate);
                    float score = 0;
                    for (i = 0; i < enums.size(); ++i) {
                        score += enums[i].q_weight * scorer_type::doc_term_weight
                            (enums[i].docs_enum.freq(), norm_len);
                    }

                    m_topk.insert(score);
                    enums[0].docs_enum.next();
                    candidate = enums[0].docs_enum.docid();
                    i = 1;
                }
            }

            m_topk.finalize();
            return m_topk.topk().size();
        }

        std::vector<float> const& topk() const
        {
            return m_topk.topk();
        }

    private:
        wand_data<scorer_type> const* m_wdata;
        topk_queue m_topk;
    };


    struct ranked_or_query {

        typedef bm25 scorer_type;

        ranked_or_query(wand_data<scorer_type> const& wdata, uint64_t k)
            : m_wdata(&wdata)
            , m_topk(k)
        {}

        template <typename Index>
        uint64_t operator()(Index const& index, term_id_vec terms)
        {
            m_topk.clear();
            if (terms.empty()) return 0;

            auto query_term_freqs = query_freqs(terms);

            uint64_t num_docs = index.num_docs();
            typedef typename Index::document_enumerator enum_type;
            struct scored_enum {
                enum_type docs_enum;
                float q_weight;
            };

            std::vector<scored_enum> enums;
            enums.reserve(query_term_freqs.size());

            for (auto term: query_term_freqs) {
                auto list = index[term.first];
                auto q_weight = scorer_type::query_term_weight
                    (term.second, list.size(), num_docs);
                enums.push_back(scored_enum {std::move(list), q_weight});
            }

            uint64_t cur_doc =
                std::min_element(enums.begin(), enums.end(),
                                 [](scored_enum const& lhs, scored_enum const& rhs) {
                                     return lhs.docs_enum.docid() < rhs.docs_enum.docid();
                                 })
                ->docs_enum.docid();

            while (cur_doc < index.num_docs()) {
                float score = 0;
                float norm_len = m_wdata->norm_len(cur_doc);
                uint64_t next_doc = index.num_docs();
                for (size_t i = 0; i < enums.size(); ++i) {
                    if (enums[i].docs_enum.docid() == cur_doc) {
                        score += enums[i].q_weight * scorer_type::doc_term_weight
                            (enums[i].docs_enum.freq(), norm_len);
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

        std::vector<float> const& topk() const
        {
            return m_topk.topk();
        }

    private:
        wand_data<scorer_type> const* m_wdata;
        topk_queue m_topk;
    };

    struct maxscore_query {

        typedef bm25 scorer_type;

        maxscore_query(wand_data<scorer_type> const& wdata, uint64_t k)
            : m_wdata(&wdata)
            , m_topk(k)
        {}

        template <typename Index>
        uint64_t operator()(Index const& index, term_id_vec const& terms)
        {
            m_topk.clear();
            if (terms.empty()) return 0;

            auto query_term_freqs = query_freqs(terms);

            uint64_t num_docs = index.num_docs();
            typedef typename Index::document_enumerator enum_type;
            struct scored_enum {
                enum_type docs_enum;
                float q_weight;
                float max_weight;
            };

            std::vector<scored_enum> enums;
            enums.reserve(query_term_freqs.size());

            for (auto term: query_term_freqs) {
                auto list = index[term.first];
                auto q_weight = scorer_type::query_term_weight
                    (term.second, list.size(), num_docs);
                auto max_weight = q_weight * m_wdata->max_term_weight(term.first);
                enums.push_back(scored_enum {std::move(list), q_weight, max_weight});
            }

            std::vector<scored_enum*> ordered_enums;
            ordered_enums.reserve(enums.size());
            for (auto& en: enums) {
                ordered_enums.push_back(&en);
            }

            // sort enumerators by increasing maxscore
            std::sort(ordered_enums.begin(), ordered_enums.end(),
                      [](scored_enum* lhs, scored_enum* rhs) {
                          return lhs->max_weight < rhs->max_weight;
                      });

            std::vector<float> upper_bounds(ordered_enums.size());
            upper_bounds[0] = ordered_enums[0]->max_weight;
            for (size_t i = 1; i < ordered_enums.size(); ++i) {
                upper_bounds[i] = upper_bounds[i - 1] + ordered_enums[i]->max_weight;
            }

            uint64_t non_essential_lists = 0;
            uint64_t cur_doc =
                std::min_element(enums.begin(), enums.end(),
                                 [](scored_enum const& lhs, scored_enum const& rhs) {
                                     return lhs.docs_enum.docid() < rhs.docs_enum.docid();
                                 })
                ->docs_enum.docid();

            while (non_essential_lists < ordered_enums.size() &&
                   cur_doc < index.num_docs()) {
                float score = 0;
                float norm_len = m_wdata->norm_len(cur_doc);
                uint64_t next_doc = index.num_docs();
                for (size_t i = non_essential_lists; i < ordered_enums.size(); ++i) {
                    if (ordered_enums[i]->docs_enum.docid() == cur_doc) {
                        score += ordered_enums[i]->q_weight * scorer_type::doc_term_weight
                            (ordered_enums[i]->docs_enum.freq(), norm_len);
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
                        score += ordered_enums[i]->q_weight * scorer_type::doc_term_weight
                            (ordered_enums[i]->docs_enum.freq(), norm_len);
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

        std::vector<float> const& topk() const
        {
            return m_topk.topk();
        }

    private:
        wand_data<scorer_type> const* m_wdata;
        topk_queue m_topk;
    };


}
