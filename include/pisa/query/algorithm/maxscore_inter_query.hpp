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
#include "cursor/inspecting_cursor.hpp"
#include "cursor/lookup_transform.hpp"
#include "cursor/max_scored_cursor.hpp"
#include "cursor/numbered_cursor.hpp"
#include "cursor/union_lookup_join.hpp"
#include "cursor/wand_join.hpp"
#include "topk_queue.hpp"
#include "util/compiler_attribute.hpp"

namespace pisa {

template <typename Cursor, typename TransformFn>
struct TransformPayloadCursor {
    constexpr TransformPayloadCursor(Cursor cursor, TransformFn transform)
        : m_cursor(std::move(cursor)), m_transform(std::move(transform))
    {}
    TransformPayloadCursor(TransformPayloadCursor&&) noexcept = default;
    TransformPayloadCursor(TransformPayloadCursor const&) = default;
    TransformPayloadCursor& operator=(TransformPayloadCursor&&) noexcept = default;
    TransformPayloadCursor& operator=(TransformPayloadCursor const&) = default;
    ~TransformPayloadCursor() = default;

    [[nodiscard]] constexpr auto docid() const noexcept { return m_cursor.docid(); }
    [[nodiscard]] constexpr auto score() { return m_transform(m_cursor); }
    [[nodiscard]] constexpr auto payload() { return m_transform(m_cursor); }
    constexpr void next() { m_cursor.next(); }
    // constexpr void advance_to_position(std::size_t pos) { m_cursor.advance_to_position(pos); }
    [[nodiscard]] constexpr auto empty() const noexcept -> bool { return m_cursor.empty(); }
    [[nodiscard]] constexpr auto position() const noexcept -> std::size_t
    {
        return m_cursor.position();
    }
    [[nodiscard]] constexpr auto size() const -> std::size_t { return m_cursor.size(); }
    [[nodiscard]] constexpr auto sentinel() const { return m_cursor.universe(); }
    [[nodiscard]] constexpr auto universe() const { return m_cursor.universe(); }

  private:
    Cursor m_cursor;
    TransformFn m_transform;
};

template <typename Cursor, typename TransformFn>
auto transform_payload(Cursor cursor, TransformFn transform)
{
    return TransformPayloadCursor<Cursor, TransformFn>(std::move(cursor), std::move(transform));
}

struct maxscore_inter_query {
    explicit maxscore_inter_query(topk_queue& topk) : m_topk(topk) {}

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
        auto term_ids = query.term_ids();

        auto term_position = [&term_ids](TermId term_id) {
            auto pos = std::find(term_ids.begin(), term_ids.end(), term_id);
            if (pos == term_ids.end()) {
                throw std::runtime_error("Programmatical error: asked for unknown term ID");
            }
            return static_cast<std::size_t>(std::distance(term_ids.begin(), pos));
        };

        auto is_above_threshold = [this](auto score) { return m_topk.would_enter(score); };
        auto selection = query.selection();
        if (not selection) {
            throw std::invalid_argument("maxscore_inter_query requires posting list selections");
        }
        auto essential_terms = selection->selected_terms;
        ranges::sort(essential_terms);
        auto non_essential_terms =
            ranges::views::set_difference(query.term_ids(), essential_terms) | ranges::to_vector;

        auto lookup_cursors = [&]() {
            auto lookup_query = QueryContainer::from_term_ids(non_essential_terms);
            std::vector<std::size_t> term_positions(non_essential_terms.size());
            ranges::transform(non_essential_terms, term_positions.begin(), term_position);
            auto cursors = number_cursors(
                make_max_scored_cursors(index, wdata, scorer, lookup_query.query(query.k())),
                term_positions);
            ranges::sort(cursors, std::greater{}, func::max_score{});
            return cursors;
        }();

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

        auto unigram_cursor = join_union_lookup(
            inspect_cursors(make_max_scored_cursors(
                index, wdata, scorer, QueryContainer::from_term_ids(essential_terms).query(query.k()))),
            gsl::make_span(lookup_cursors),
            0.0F,
            Add{},
            is_above_threshold,
            max_docid);

        using lookup_cursor_type = std::conditional_t<
            std::is_void_v<Inspect>,
            std::decay_t<decltype(lookup_cursors[0])>,
            InspectingCursor<std::decay_t<decltype(lookup_cursors[0])>, Inspect>>;
        using bigram_cursor_type = std::conditional_t<
            std::is_void_v<Inspect>,
            PairMaxScoredCursor<typename std::decay_t<PairIndex>::cursor_type>,
            InspectingCursor<PairMaxScoredCursor<typename std::decay_t<PairIndex>::cursor_type>, Inspect>>;

        using lookup_transform_type =
            LookupTransform<lookup_cursor_type, decltype(is_above_threshold)>;
        using transform_payload_cursor_type =
            TransformPayloadCursor<bigram_cursor_type, lookup_transform_type>;

        std::vector<transform_payload_cursor_type> bigram_cursors;

        for (auto [left, right]: selection->selected_pairs) {
            auto pair_id = pair_index.pair_id(left, right);
            if (not pair_id) {
                throw std::runtime_error(fmt::format("Pair not found: <{}, {}>", left, right));
            }
            auto cursor = inspect_cursor(make_max_scored_pair_cursor(
                pair_index.index(), wdata, *pair_id, scorer, left, right));
            std::vector<TermId> v{left, right};
            auto bigram_lookup_cursors =
                ranges::views::set_difference(non_essential_terms, v)
                | ranges::views::transform([&](auto term_id) {
                      return NumberedCursor(
                          make_max_scored_cursor(index, wdata, scorer, term_id),
                          term_position(term_id));
                  })

                | ranges::to_vector;
            auto lookup_cursors_upper_bound = std::accumulate(
                bigram_lookup_cursors.begin(),
                bigram_lookup_cursors.end(),
                0.0F,
                [](auto acc, auto&& cursor) { return acc + cursor.max_score(); });
            bigram_cursors.emplace_back(
                std::move(cursor),
                LookupTransform(
                    inspect_cursors(std::move(bigram_lookup_cursors)),
                    lookup_cursors_upper_bound,
                    is_above_threshold));
        }

        auto accumulate = [&](float acc, auto& cursor) { return acc == 0 ? cursor.score() : acc; };

        auto bigram_cursor = union_merge(std::move(bigram_cursors), 0.0F, accumulate);
        auto merged = variadic_union_merge(
            0.0F,
            std::make_tuple(std::move(unigram_cursor), std::move(bigram_cursor)),
            std::make_tuple(accumulate, accumulate));

        while (not merged.empty()) {
            m_topk.insert(merged.payload(), merged.docid());
            merged.next();
        }
    }

    std::vector<std::pair<float, uint64_t>> const& topk() const { return m_topk.topk(); }

  private:
    topk_queue& m_topk;
};

}  // namespace pisa
