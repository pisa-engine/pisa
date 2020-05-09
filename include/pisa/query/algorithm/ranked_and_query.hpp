#pragma once

#include <vector>

#include "cursor/cursor_intersection.hpp"
#include "topk_queue.hpp"

namespace pisa {

struct ranked_and_query {
    explicit ranked_and_query(topk_queue& topk) : m_topk(topk) {}

    template <typename CursorRange>
    void operator()(CursorRange&& cursors, uint64_t max_docid)
    {
        auto intersection = intersect(
            std::move(cursors),
            0.0,
            [](auto score, auto&& cursor) { return score + cursor.score(); },
            std::optional<std::uint32_t>(max_docid));

        while (not intersection.empty()) {
            m_topk.insert(intersection.payload(), intersection.docid());
            intersection.next();
        }
    }

    std::vector<std::pair<float, uint64_t>> const& topk() const { return m_topk.topk(); }

    topk_queue& get_topk() { return m_topk; }

  private:
    topk_queue& m_topk;
};

}  // namespace pisa
