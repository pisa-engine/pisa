#pragma once

#include "query/queries.hpp"
#include <vector>

namespace pisa {

struct block_max_ranked_and_query {

    block_max_ranked_and_query(topk_queue& topk) : m_topk(topk) {}

    template <typename CursorRange>
    uint64_t operator()(CursorRange &&cursors, uint64_t max_docid)
    {
        using Cursor = typename std::decay_t<CursorRange>::value_type;

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
        size_t candidate_list = 1;
        while (candidate < max_docid) {

            // Get current block UB
            double block_upper_bound = 0;
            for (size_t block = 0; block < ordered_cursors.size(); ++block) {
                ordered_cursors[block]->w.next_geq(candidate);
                block_upper_bound +=
                    ordered_cursors[block]->w.score() * ordered_cursors[block]->q_weight;
            }
            if (m_topk.would_enter(block_upper_bound)) {

                for (; candidate_list < ordered_cursors.size(); ++candidate_list) {
                    ordered_cursors[candidate_list]->docs_enum.next_geq(candidate);

                    if (ordered_cursors[candidate_list]->docs_enum.docid() != candidate) {
                        candidate = ordered_cursors[candidate_list]->docs_enum.docid();
                        candidate_list = 0;
                        break;
                    }
                }
                if (candidate_list == ordered_cursors.size()) {
                    float score = 0;
                    for (candidate_list = 0; candidate_list < ordered_cursors.size();
                         ++candidate_list) {
                        score += ordered_cursors[candidate_list]->scorer(
                            ordered_cursors[candidate_list]->docs_enum.docid(),
                            ordered_cursors[candidate_list]->docs_enum.freq());
                    }

                    m_topk.insert(score, ordered_cursors[0]->docs_enum.docid());
                    ordered_cursors[0]->docs_enum.next();
                    candidate = ordered_cursors[0]->docs_enum.docid();
                    candidate_list = 1;
                }
            } else {
                candidate_list = 0;
                uint64_t next_jump = max_docid;
                for (size_t block = 0; block < ordered_cursors.size(); ++block) {
                    next_jump = std::min(next_jump, ordered_cursors[block]->w.docid());
                }
                // We have exhausted a list, so we are done
                if (candidate == next_jump + 1)
                    candidate = max_docid;
                // Otherwise, exit the current block configuration
                else
                    candidate = next_jump + 1;
            }
        }

        m_topk.finalize();
        return m_topk.topk().size();
    }

    std::vector<std::pair<float, uint64_t>> const &topk() const { return m_topk.topk(); }

    topk_queue &get_topk() { return m_topk; }

   private:
    topk_queue& m_topk;
};

} // namespace pisa
