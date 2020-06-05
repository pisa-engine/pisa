#pragma once

#include <vector>

#include "cursor/cursor.hpp"
#include "cursor/wand_join.hpp"
#include "query/queries.hpp"
#include "topk_queue.hpp"

namespace pisa {

struct block_max_wand_query {
    explicit block_max_wand_query(topk_queue& topk) : m_topk(topk) {}

    template <typename CursorRange, typename Inspect = void>
    void operator()(CursorRange&& cursors, uint64_t max_docid, Inspect* inspect = nullptr)
    {
        using Cursor = typename std::decay_t<CursorRange>::value_type;
        if (cursors.empty()) {
            return;
        }

        auto joined = join_block_max_wand(
            std::move(cursors),
            0.0,
            Add{},
            [&](auto score) { return m_topk.would_enter(score); },
            max_docid);
        while (not joined.empty() && joined.docid() < max_docid) {
            m_topk.insert(joined.payload(), joined.docid());
            joined.next();
        }
    }

    std::vector<std::pair<float, uint64_t>> const& topk() const { return m_topk.topk(); }

    void clear_topk() { m_topk.clear(); }

    topk_queue const& get_topk() const { return m_topk; }

  private:
    topk_queue& m_topk;
};

}  // namespace pisa
