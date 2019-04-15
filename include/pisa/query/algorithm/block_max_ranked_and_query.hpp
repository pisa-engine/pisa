#pragma once

#include <vector>
#include "query/queries.hpp"

namespace pisa {

struct block_max_ranked_and_query {

    typedef bm25 scorer_type;

    block_max_ranked_and_query(uint64_t k) : m_topk(k) {}

    template <typename CursorRange>
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


        // sort by increasing frequency
        std::sort(ordered_cursors.begin(), ordered_cursors.end(), [](Cursor *lhs, Cursor *rhs) {
            return lhs->docs_enum.size() < rhs->docs_enum.size();
        });

        uint64_t candidate = ordered_cursors[0]->docs_enum.docid();
        size_t   i         = 1;
        while (candidate < max_docid) {

            // Get current block UB
            double block_upper_bound = 0;
            for (size_t block = 0; block < ordered_cursors.size(); ++block) {
                ordered_cursors[block]->w.next_geq(candidate);
                block_upper_bound += ordered_cursors[block]->w.score() * ordered_cursors[block]->q_weight;
            }
            if (m_topk.would_enter(block_upper_bound)) {

                for (; i < ordered_cursors.size(); ++i) {
                    ordered_cursors[i]->docs_enum.next_geq(candidate);

                    if (ordered_cursors[i]->docs_enum.docid() != candidate) {
                        candidate = ordered_cursors[i]->docs_enum.docid();
                        i         = 0;
                        break;
                    }
                }

                if (i == ordered_cursors.size()) {
                    float score    = 0;
                    for (i = 0; i < ordered_cursors.size(); ++i) {
                        score += ordered_cursors[i]->scorer(ordered_cursors[i]->docs_enum.docid(), ordered_cursors[i]->docs_enum.freq());
                    }

                    m_topk.insert(score, ordered_cursors[0]->docs_enum.docid());
                    ordered_cursors[0]->docs_enum.next();
                    candidate = ordered_cursors[0]->docs_enum.docid();
                    i         = 1;
                }
            }
            else {

                ordered_cursors[0]->docs_enum.next();
                // uint64_t next_jump = ordered_cursors[0]->docs_enum.docid();

                // // Next jump is the last identifier in the current block arrangement
                // for (size_t j = 1; j < ordered_cursors.size(); ++j) {
                //     next_jump = std::max(ordered_cursors[j]->w.docid(), next_jump);
                // }
                // if (next_jump + 1 > ordered_cursors[0]->docs_enum.docid())
                //   ordered_cursors[0]->docs_enum.next_geq(next_jump + 1);
                candidate = ordered_cursors[0]->docs_enum.docid()+1;
            }
        }

        m_topk.finalize();
        return m_topk.topk().size();
    }

    std::vector<std::pair<float, uint64_t>> const &topk() const { return m_topk.topk(); }

    topk_queue &get_topk() { return m_topk; }

   private:
    topk_queue      m_topk;
};

} // namespace pisa
