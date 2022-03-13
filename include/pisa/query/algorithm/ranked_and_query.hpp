#pragma once

#include "query/queries.hpp"
#include "topk_queue.hpp"
#include <vector>

namespace pisa {

struct ranked_and_query {
    explicit ranked_and_query(topk_queue& topk) : m_topk(topk) {}

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
        size_t i = 1;
        while (candidate < max_docid) {
            for (; i < ordered_cursors.size(); ++i) {
                ordered_cursors[i]->next_geq(candidate);
                if (ordered_cursors[i]->docid() != candidate) {
                    candidate = ordered_cursors[i]->docid();
                    i = 0;
                    break;
                }
            }

            if (i == ordered_cursors.size()) {
                float score = 0;
                for (i = 0; i < ordered_cursors.size(); ++i) {
                    score += ordered_cursors[i]->score();
                }

                m_topk.insert(score, ordered_cursors[0]->docid());
                ordered_cursors[0]->next();
                candidate = ordered_cursors[0]->docid();
                i = 1;
            }
        }
    }

    std::vector<typename topk_queue::entry_type> const& topk() const { return m_topk.topk(); }

    topk_queue& get_topk() { return m_topk; }

  private:
    topk_queue& m_topk;
};

}  // namespace pisa
