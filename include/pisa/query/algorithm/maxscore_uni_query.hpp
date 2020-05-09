#pragma once

#include <vector>

#include <range/v3/view/reverse.hpp>

#include "cursor/union_lookup_join.hpp"
#include "topk_queue.hpp"
#include "util/compiler_attribute.hpp"

namespace pisa {

struct Add {
    template <typename Score, typename Cursor>
    PISA_ALWAYSINLINE auto operator()(Score&& score, Cursor&& cursor)
    {
        score += cursor.score();
        return score;
    }
};

struct maxscore_uni_query {
    explicit maxscore_uni_query(topk_queue& topk) : m_topk(topk) {}

    template <typename CursorRange>
    void operator()(CursorRange&& cursors, uint64_t max_docid)
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
            std::move(essential_cursors),
            std::move(non_essential_cursors),
            0.0,
            Add{},
            [&](auto score) { return m_topk.would_enter(score); },
            max_docid);
        while (not joined.empty()) {
            m_topk.insert(joined.payload(), joined.docid());
            joined.next();
        }

        // using Cursor = typename std::decay_t<CursorRange>::value_type;
        // if (cursors.empty()) {
        //    return;
        //}

        // std::vector<Cursor*> ordered_cursors;
        // ordered_cursors.reserve(cursors.size());
        // for (auto& en: cursors) {
        //    ordered_cursors.push_back(&en);
        //}

        //// sort enumerators by increasing maxscore
        // std::sort(ordered_cursors.begin(), ordered_cursors.end(), [](Cursor* lhs, Cursor* rhs) {
        //    return lhs->max_score() < rhs->max_score();
        //});

        // std::vector<float> upper_bounds(ordered_cursors.size());
        // upper_bounds[0] = ordered_cursors[0]->max_score();
        // for (size_t i = 1; i < ordered_cursors.size(); ++i) {
        //    upper_bounds[i] = upper_bounds[i - 1] + ordered_cursors[i]->max_score();
        //}

        // uint64_t non_essential_lists = 0;
        // auto update_non_essential_lists = [&]() {
        //    while (non_essential_lists < ordered_cursors.size()
        //           && !m_topk.would_enter(upper_bounds[non_essential_lists])) {
        //        non_essential_lists += 1;
        //    }
        //};
        // update_non_essential_lists();
        // std::cout << "nonessential: " << non_essential_lists << '\n';

        // uint64_t cur_doc =
        //    std::min_element(cursors.begin(), cursors.end(), [](Cursor const& lhs, Cursor const&
        //    rhs) {
        //        return lhs.docid() < rhs.docid();
        //    })->docid();

        // while (cur_doc < max_docid) {
        //    float score = 0;
        //    uint64_t next_doc = max_docid;
        //    for (size_t i = non_essential_lists; i < ordered_cursors.size(); ++i) {
        //        if (ordered_cursors[i]->docid() == cur_doc) {
        //            score += ordered_cursors[i]->score();
        //            ordered_cursors[i]->next();
        //        }
        //        if (ordered_cursors[i]->docid() < next_doc) {
        //            next_doc = ordered_cursors[i]->docid();
        //        }
        //    }

        //    // try to complete evaluation with non-essential lists
        //    for (size_t i = non_essential_lists - 1; i + 1 > 0; --i) {
        //        if (!m_topk.would_enter(score + upper_bounds[i])) {
        //            break;
        //        }
        //        ordered_cursors[i]->next_geq(cur_doc);
        //        if (ordered_cursors[i]->docid() == cur_doc) {
        //            score += ordered_cursors[i]->score();
        //        }
        //    }

        //    m_topk.insert(score, cur_doc);
        //    cur_doc = next_doc;
        //}
    }

    std::vector<std::pair<float, uint64_t>> const& topk() const { return m_topk.topk(); }

  private:
    topk_queue& m_topk;
};

}  // namespace pisa
