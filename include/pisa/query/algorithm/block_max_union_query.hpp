#pragma once

#include <optional>
#include <vector>

#include "topk_queue.hpp"

#include "cursor/block_max_union.hpp"

namespace pisa {

struct BlockMaxUnionQuery {
    explicit BlockMaxUnionQuery(topk_queue& topk) : m_topk(topk) {}

    template <typename CursorRange>
    void operator()(CursorRange&& cursors, uint64_t max_docid)
    {
        auto postings = block_max_union(
            std::move(cursors),
            0.0,
            [](auto score, auto&& cursor) { return score + cursor.score(); },
            [&](auto score) { return m_topk.would_enter(score); },
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
