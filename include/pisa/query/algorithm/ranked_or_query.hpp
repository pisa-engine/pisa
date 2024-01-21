#pragma once

#include <vector>

#include "concepts.hpp"
#include "concepts/posting_cursor.hpp"
#include "topk_queue.hpp"

namespace pisa {

/**
 * Top-k disjunctive retrieval.
 *
 * Returns the top-k highest scored documents matching at least one query term.
 * This algorithm exhaustively scores every single document in the posting list union.
 */
struct ranked_or_query {
    explicit ranked_or_query(topk_queue& topk) : m_topk(topk) {}

    template <typename CursorRange>
    PISA_REQUIRES(
        (concepts::ScoredPostingCursor<pisa::val_t<CursorRange>>
         && concepts::SortedPostingCursor<pisa::val_t<CursorRange>>)
    )
    void
    operator()(CursorRange&& cursors, uint64_t max_docid) {
        using Cursor = typename std::decay_t<CursorRange>::value_type;
        if (cursors.empty()) {
            return;
        }
        uint64_t cur_doc =
            std::min_element(cursors.begin(), cursors.end(), [](Cursor const& lhs, Cursor const& rhs) {
                return lhs.docid() < rhs.docid();
            })->docid();

        while (cur_doc < max_docid) {
            float score = 0;
            uint64_t next_doc = max_docid;
            for (size_t i = 0; i < cursors.size(); ++i) {
                if (cursors[i].docid() == cur_doc) {
                    score += cursors[i].score();
                    cursors[i].next();
                }
                if (cursors[i].docid() < next_doc) {
                    next_doc = cursors[i].docid();
                }
            }

            m_topk.insert(score, cur_doc);
            cur_doc = next_doc;
        }
    }

    std::vector<typename topk_queue::entry_type> const& topk() const { return m_topk.topk(); }

  private:
    topk_queue& m_topk;
};

}  // namespace pisa
