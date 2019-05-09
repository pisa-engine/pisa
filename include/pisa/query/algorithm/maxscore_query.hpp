#pragma once

#include <vector>

#include "query/queries.hpp"
#include "scorer/bm25.hpp"
#include "topk_queue.hpp"

namespace pisa {

struct maxscore_query {

    typedef bm25 scorer_type;

    maxscore_query(uint64_t k)
    : m_topk(k) {}

    template<typename CursorRange>
    uint64_t operator()(CursorRange &&cursors, uint64_t max_docid) {
        using Cursor = typename std::decay_t<CursorRange>::value_type;
        m_topk.clear();
        if (cursors.empty())
            return 0;

        std::vector<Cursor *> ordered_cursors;
        ordered_cursors.reserve(cursors.size());
        for (auto &en : cursors) {
            ordered_cursors.push_back(&en);
        }

        // sort enumerators by increasing maxscore
        std::sort(
            ordered_cursors.begin(), ordered_cursors.end(), [](Cursor *lhs, Cursor *rhs) {
                return lhs->max_weight < rhs->max_weight;
            });

        std::vector<float> upper_bounds(ordered_cursors.size());
        upper_bounds[0] = ordered_cursors[0]->max_weight;
        for (size_t i = 1; i < ordered_cursors.size(); ++i) {
            upper_bounds[i] = upper_bounds[i - 1] + ordered_cursors[i]->max_weight;
        }

        uint64_t non_essential_lists = 0;
        uint64_t cur_doc =
            std::min_element(cursors.begin(),
                             cursors.end(),
                             [](Cursor const &lhs, Cursor const &rhs) {
                                 return lhs.docs_enum.docid() < rhs.docs_enum.docid();
                             })
                ->docs_enum.docid();

        while (non_essential_lists < ordered_cursors.size() && cur_doc < max_docid) {
            float    score    = 0;
            uint64_t next_doc = max_docid;
            for (size_t i = non_essential_lists; i < ordered_cursors.size(); ++i) {
                if (ordered_cursors[i]->docs_enum.docid() == cur_doc) {
                    score += ordered_cursors[i]->scorer(ordered_cursors[i]->docs_enum.docid(), ordered_cursors[i]->docs_enum.freq());
                    ordered_cursors[i]->docs_enum.next();
                }
                if (ordered_cursors[i]->docs_enum.docid() < next_doc) {
                    next_doc = ordered_cursors[i]->docs_enum.docid();
                }
            }

            // try to complete evaluation with non-essential lists
            for (size_t i = non_essential_lists - 1; i + 1 > 0; --i) {
                if (!m_topk.would_enter(score + upper_bounds[i])) {
                    break;
                }
                ordered_cursors[i]->docs_enum.next_geq(cur_doc);
                if (ordered_cursors[i]->docs_enum.docid() == cur_doc) {
                    score += ordered_cursors[i]->scorer(ordered_cursors[i]->docs_enum.docid(), ordered_cursors[i]->docs_enum.freq());
                }
            }

            if (m_topk.insert(score, cur_doc)) {
                // update non-essential lists
                while (non_essential_lists < ordered_cursors.size() &&
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
    topk_queue      m_topk;
};

} // namespace pisa
