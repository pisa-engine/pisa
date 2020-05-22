#pragma once

#include <numeric>
#include <vector>

#include <range/v3/view/reverse.hpp>

#include "cursor/cursor.hpp"
#include "cursor/cursor_union.hpp"
#include "cursor/inspecting_cursor.hpp"
#include "cursor/union_lookup_join.hpp"
#include "cursor/wand_join.hpp"
#include "topk_queue.hpp"
#include "util/compiler_attribute.hpp"

namespace pisa {

struct maxscore_uni_query {
    explicit maxscore_uni_query(topk_queue& topk) : m_topk(topk) {}

    template <typename CursorRange, typename Inspect = void>
    void operator()(CursorRange&& cursors, uint64_t max_docid, Inspect* inspect = nullptr)
    {
        using cursor_type = typename std::decay_t<CursorRange>::value_type;
        if (cursors.empty()) {
            return;
        }

        std::vector<std::size_t> term_positions(cursors.size());
        std::iota(term_positions.begin(), term_positions.end(), 0);

        auto [non_essential, essential] = maxscore_partition(
            gsl::make_span(term_positions), m_topk.threshold(), [&](auto term_position) {
                return cursors[term_position].max_score();
            });

        std::vector<cursor_type> essential_cursors;
        std::vector<cursor_type> non_essential_cursors;
        for (auto pos: essential) {
            essential_cursors.push_back(std::move(cursors[pos]));
        }
        for (auto pos: ranges::views::reverse(non_essential)) {
            non_essential_cursors.push_back(std::move(cursors[pos]));
        }

        auto joined = join_union_lookup(
            union_merge(std::move(essential_cursors), 0.0, Add{}, max_docid),
            std::move(non_essential_cursors),
            0.0,
            Add{},
            [&](auto score) { return m_topk.would_enter(score); },
            max_docid);
        while (not joined.empty()) {
            m_topk.insert(joined.score(), joined.docid());
            joined.next();
        }
    }

    std::vector<std::pair<float, uint64_t>> const& topk() const { return m_topk.topk(); }

  private:
    topk_queue& m_topk;
};

}  // namespace pisa
