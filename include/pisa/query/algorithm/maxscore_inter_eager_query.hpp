#pragma once

#include <numeric>
#include <vector>

#include <fmt/format.h>
#include <range/v3/algorithm/transform.hpp>
#include <range/v3/view/filter.hpp>
#include <range/v3/view/reverse.hpp>
#include <range/v3/view/set_algorithm.hpp>
#include <range/v3/view/transform.hpp>

#include "cursor/cursor.hpp"
#include "cursor/cursor_union.hpp"
#include "cursor/lookup_transform.hpp"
#include "cursor/max_scored_cursor.hpp"
#include "cursor/numbered_cursor.hpp"
#include "cursor/union_lookup_join.hpp"
#include "cursor/wand_join.hpp"
#include "query/algorithm/maxscore_inter_query.hpp"
#include "topk_queue.hpp"
#include "util/compiler_attribute.hpp"

namespace pisa {

template <typename Cursor>
auto accumulate_cursor_to_heap(Cursor&& cursor, std::size_t k, float threshold, TermId sentinel)
{
    topk_queue heap(k);
    heap.set_threshold(threshold);
    while (cursor.docid() < sentinel) {
        heap.insert(cursor.score(), cursor.docid());
        cursor.next();
    }
    return heap;
}

struct maxscore_inter_eager_query {
    explicit maxscore_inter_eager_query(topk_queue& topk) : m_topk(topk) {}

    template <typename Index, typename Wand, typename PairIndex, typename Scorer, typename Inspect = void>
    void operator()(
        QueryRequest const query,
        Index&& index,
        Wand&& wdata,
        PairIndex&& pair_index,
        Scorer&& scorer,
        std::uint32_t max_docid,
        Inspect* inspect = nullptr)
    {
        using lookup_cursor_type = std::conditional_t<
            std::is_void_v<Inspect>,
            std::decay_t<decltype(make_max_scored_cursor(index, wdata, scorer, std::declval<TermId>()))>,
            InspectingCursor<
                std::decay_t<decltype(make_max_scored_cursor(index, wdata, scorer, std::declval<TermId>()))>,
                Inspect>>;
        using bigram_cursor_type = std::conditional_t<
            std::is_void_v<Inspect>,
            PairMaxScoredCursor<typename std::decay_t<PairIndex>::cursor_type>,
            InspectingCursor<PairMaxScoredCursor<typename std::decay_t<PairIndex>::cursor_type>, Inspect>>;

        auto inspect_cursors = [&](auto&& cursors) {
            if constexpr (std::is_void_v<Inspect>) {
                return std::forward<decltype(cursors)>(cursors);
            } else {
                return ::pisa::inspect_cursors(std::forward<decltype(cursors)>(cursors), *inspect);
            }
        };

        auto inspect_cursor = [&](auto&& cursors) {
            if constexpr (std::is_void_v<Inspect>) {
                return std::forward<decltype(cursors)>(cursors);
            } else {
                return ::pisa::inspect_cursor(std::forward<decltype(cursors)>(cursors), *inspect);
            }
        };

        auto term_ids = query.term_ids();

        auto is_above_threshold = [this](auto score) { return m_topk.would_enter(score); };
        auto selection = query.selection();
        if (not selection) {
            throw std::invalid_argument("maxscore_inter_query requires posting list selections");
        }
        auto essential_terms = selection->selected_terms;
        auto non_essential_terms =
            ranges::views::set_difference(term_ids, essential_terms) | ranges::to_vector;

        auto unigram_cursor = [&]() {
            auto lookup_cursors = make_max_scored_cursors(
                index,
                wdata,
                scorer,
                QueryContainer::from_term_ids(non_essential_terms).query(query.k()));
            ranges::sort(lookup_cursors, std::greater{}, func::max_score{});
            auto essential_cursors = make_max_scored_cursors(
                index, wdata, scorer, QueryContainer::from_term_ids(essential_terms).query(query.k()));
            return join_union_lookup(
                inspect_cursors(std::move(essential_cursors)),
                inspect_cursors(std::move(lookup_cursors)),
                0.0F,
                Add{},
                is_above_threshold,
                max_docid);
        }();
        auto unigram_heap =
            accumulate_cursor_to_heap(unigram_cursor, query.k(), m_topk.threshold(), max_docid);

        using lookup_transform_type =
            LookupTransform<lookup_cursor_type, decltype(is_above_threshold)>;
        using transform_payload_cursor_type =
            TransformPayloadCursor<bigram_cursor_type, lookup_transform_type>;

        std::vector<typename topk_queue::entry_type> entries(
            unigram_heap.topk().begin(), unigram_heap.topk().end());

        for (auto [left, right]: selection->selected_pairs) {
            auto pair_id = pair_index.pair_id(left, right);
            if (not pair_id) {
                throw std::runtime_error(fmt::format("Pair not found: <{}, {}>", left, right));
            }
            auto cursor = inspect_cursor(make_max_scored_pair_cursor(
                pair_index.index(), wdata, *pair_id, scorer, left, right));

            std::vector<TermId> essential_terms{left, right};
            auto lookup_terms = ranges::views::set_difference(non_essential_terms, essential_terms)
                | ranges::to_vector;

            auto lookup_cursors = make_max_scored_cursors(
                index, wdata, scorer, QueryContainer::from_term_ids(lookup_terms).query(query.k()));
            ranges::sort(lookup_cursors, [](auto&& lhs, auto&& rhs) {
                return lhs.max_score() > rhs.max_score();
            });

            auto lookup_cursors_upper_bound = std::accumulate(
                lookup_cursors.begin(), lookup_cursors.end(), 0.0F, [](auto acc, auto&& cursor) {
                    return acc + cursor.max_score();
                });

            auto heap = accumulate_cursor_to_heap(
                transform_payload_cursor_type(
                    std::move(cursor),
                    lookup_transform_type(
                        inspect_cursors(std::move(lookup_cursors)),
                        lookup_cursors_upper_bound,
                        is_above_threshold)),
                query.k(),
                m_topk.threshold(),
                max_docid);
            std::copy(heap.topk().begin(), heap.topk().end(), std::back_inserter(entries));
        }
        std::sort(entries.begin(), entries.end(), [](auto&& lhs, auto&& rhs) {
            if (lhs.second == rhs.second) {
                return lhs.first > rhs.first;
            }
            return lhs.second < rhs.second;
        });
        auto end = std::unique(entries.begin(), entries.end(), [](auto&& lhs, auto&& rhs) {
            return lhs.second == rhs.second;
        });
        entries.erase(end, entries.end());
        std::sort(entries.begin(), entries.end(), [](auto&& lhs, auto&& rhs) {
            return lhs.first > rhs.first;
        });
        if (entries.size() > m_topk.size()) {
            entries.erase(std::next(entries.begin(), m_topk.size()), entries.end());
        }

        for (auto entry: entries) {
            m_topk.insert(entry.first, entry.second);
        }
    }

    std::vector<std::pair<float, uint64_t>> const& topk() const { return m_topk.topk(); }

  private:
    topk_queue& m_topk;
};

}  // namespace pisa
