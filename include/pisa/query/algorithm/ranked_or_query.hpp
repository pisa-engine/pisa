#pragma once

#include <string>
#include <vector>

#include "cursor/cursor_union.hpp"
#include "topk_queue.hpp"

namespace pisa {

struct ranked_or_query {
    explicit ranked_or_query(topk_queue& topk) : m_topk(topk) {}

    template <typename CursorRange>
    void operator()(CursorRange&& cursors, uint64_t max_docid)
    {
        auto postings = union_merge(
            std::move(cursors),
            0.0,
            [](auto score, auto&& cursor) { return score + cursor.score(); },
            std::optional<std::uint32_t>(max_docid));

        while (not postings.empty()) {
            m_topk.insert(postings.payload(), postings.docid());
            postings.next();
        }
    }

    std::vector<std::pair<float, uint64_t>> const& topk() const { return m_topk.topk(); }

  private:
    topk_queue& m_topk;
};

}  // namespace pisa
