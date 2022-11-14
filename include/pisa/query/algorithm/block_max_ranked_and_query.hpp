#pragma once

#include "query/queries.hpp"
#include "topk_queue.hpp"
#include <vector>

namespace pisa {

struct block_max_ranked_and_query {
    explicit block_max_ranked_and_query(topk_queue& topk) : m_topk(topk) {}

    template <typename CursorRange>
    void operator()(CursorRange&& cursors, uint64_t max_docid)
    {
        using Cursor = typename std::decay_t<CursorRange>::value_type;

        if (cursors.empty()) {
            return;
        }

        std::vector<Cursor*> ordered_cursors;
        ordered_cursors.reserve(cursors.size());
        for (auto& en: cursors) {
            ordered_cursors.push_back(&en);
        }

        // sort by increasing frequency
        std::sort(ordered_cursors.begin(), ordered_cursors.end(), [](Cursor* lhs, Cursor* rhs) {
            return lhs->size() < rhs->size();
        });

        uint64_t candidate = ordered_cursors[0]->docid();
        size_t candidate_list = 1;
        while (candidate < max_docid) {
            // Get current block UB
            double block_upper_bound = 0;
            for (size_t block = 0; block < ordered_cursors.size(); ++block) {
                ordered_cursors[block]->block_max_next_geq(candidate);
                block_upper_bound += ordered_cursors[block]->block_max_score();
            }
            if (m_topk.would_enter(block_upper_bound)) {
                for (; candidate_list < ordered_cursors.size(); ++candidate_list) {
                    ordered_cursors[candidate_list]->next_geq(candidate);

                    if (ordered_cursors[candidate_list]->docid() != candidate) {
                        candidate = ordered_cursors[candidate_list]->docid();
                        candidate_list = 0;
                        break;
                    }
                }
                if (candidate_list == ordered_cursors.size()) {
                    float score = 0;
                    for (candidate_list = 0; candidate_list < ordered_cursors.size();
                         ++candidate_list) {
                        score += ordered_cursors[candidate_list]->score();
                    }

                    m_topk.insert(score, ordered_cursors[0]->docid());
                    ordered_cursors[0]->next();
                    candidate = ordered_cursors[0]->docid();
                    candidate_list = 1;
                }
            } else {
                candidate_list = 0;
                std::uint32_t next_jump = max_docid;
                for (size_t block = 0; block < ordered_cursors.size(); ++block) {
                    next_jump = std::min(next_jump, ordered_cursors[block]->block_max_docid());
                }
                if (candidate == next_jump + 1) {
                    // We have exhausted a list, so we are done
                    candidate = max_docid;
                } else {
                    // Otherwise, exit the current block configuration
                    candidate = next_jump + 1;
                }
            }
        }
    }

    std::vector<typename topk_queue::entry_type> const& topk() const { return m_topk.topk(); }

    topk_queue& get_topk() { return m_topk; }

  private:
    topk_queue& m_topk;
};

}  // namespace pisa
