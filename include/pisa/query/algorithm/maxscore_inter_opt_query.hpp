#pragma once

#include <chrono>
#include <numeric>
#include <vector>
#include <x86intrin.h>
#include <xmmintrin.h>

#include <fmt/format.h>
#include <range/v3/action/sort.hpp>
#include <range/v3/algorithm/transform.hpp>
#include <range/v3/view/filter.hpp>
#include <range/v3/view/reverse.hpp>
#include <range/v3/view/set_algorithm.hpp>
#include <range/v3/view/transform.hpp>

#include "cursor/cursor.hpp"
#include "cursor/cursor_intersection.hpp"
#include "cursor/cursor_union.hpp"
#include "cursor/inspecting_cursor.hpp"
#include "cursor/lookup_transform.hpp"
#include "cursor/max_scored_cursor.hpp"
#include "cursor/numbered_cursor.hpp"
#include "intersection.hpp"
#include "query/algorithm/block_max_wand_query.hpp"
#include "query/algorithm/maxscore_query.hpp"
#include "timer.hpp"
#include "topk_queue.hpp"
#include "util/compiler_attribute.hpp"

namespace pisa {

using ranges::to_vector;
using ranges::actions::sort;

using State = std::uint32_t;

template <std::size_t N>
PISA_ALWAYSINLINE auto precompute_next_lookup(
    std::size_t essential_count,
    std::size_t non_essential_count,
    std::vector<std::vector<std::uint32_t>> const& essential_pairs)
{
    if (essential_count + non_essential_count > N) {
        throw std::runtime_error(fmt::format("Must be shorter than {}", N));
    }
    std::uint32_t term_count = essential_count + non_essential_count;
    std::vector<std::int32_t> next_lookup((term_count + 1) * (1U << term_count), -1);
    auto unnecessary = [&](auto p, auto state) {
        if (((1U << p) & state) > 0) {
            return true;
        }
        for (auto k: essential_pairs[p]) {
            if (((1U << k) & state) > 0) {
                return true;
            }
        }
        return false;
    };
    for (auto term_idx = essential_count; term_idx < term_count; term_idx += 1) {
        for (State state = 0; state < (1U << term_count); state += 1) {
            auto p = term_idx;
            while (p < term_count && unnecessary(p, state)) {
                ++p;
            }
            if (p == term_count) {
                next_lookup[(term_idx << term_count) + state] = -1;
            } else {
                next_lookup[(term_idx << term_count) + state] = p;
            }
        }
    }
    return next_lookup;
}

struct Payload {
    State state{0};
    float score{0.0};

    PISA_ALWAYSINLINE auto operator+=(Payload const& other) -> Payload&
    {
        score += other.score * static_cast<float>((state & other.state) == 0);
        state |= other.state;
        return *this;
    }

    PISA_ALWAYSINLINE auto accumulate(State state, float score) -> Payload&
    {
        if ((this->state & state) == 0) {
            this->score += score;
            this->state |= state;
        }
        return *this;
    }
};

template <std::size_t N, typename R1, typename R2>
[[nodiscard]] PISA_ALWAYSINLINE auto term_position_function(R1&& essential_terms, R2&& lookup_terms)
{
    std::array<TermId, N> ids{};
    std::copy(essential_terms.begin(), essential_terms.end(), ids.begin());
    std::copy(
        lookup_terms.begin(), lookup_terms.end(), std::next(ids.begin(), essential_terms.size()));
    return [ids](TermId term_id) {
        auto pos = std::find(ids.begin(), std::next(ids.begin(), ids.size()), term_id);
        if (pos == ids.end()) {
            throw std::runtime_error("Programmatical error: asked for unknown term ID");
        }
        return static_cast<std::uint32_t>(std::distance(ids.begin(), pos));
    };
}

using intersection_lattice_type = IntersectionLattice<std::uint16_t>;

[[nodiscard]] auto
// select_essential_single(QueryRequest const query, Index&& index, Wand&& wdata, float threshold)
select_essential_single(QueryRequest const query, intersection_lattice_type const& lattice, float threshold)
    -> std::pair<Selection<TermId>, std::uint32_t>
{
    auto term_ids = query.term_ids();
    /* std::vector<float> max_scores(term_ids.size()); */
    /* std::generate_n(max_scores.begin(), term_ids.size(), [&](auto pos) { */
    /*     return lattice.score_bound(1U << pos); */
    /* }); */
    std::vector<std::size_t> indices(term_ids.size());
    std::iota(indices.begin(), indices.end(), 0);
    std::sort(indices.begin(), indices.end(), [&lattice](auto lhs, auto rhs) {
        return lattice.score_bound(1U << lhs) < lattice.score_bound(1U << rhs);
    });
    float sum = 0.0;
    auto first_essential =
        std::find_if(indices.begin(), indices.end(), [&lattice, &sum, threshold](auto idx) {
            sum += lattice.score_bound(1U << idx);
            return sum > threshold;
        });
    Selection<TermId> selection;
    float cost = 0.0;
    for (auto iter = first_essential; iter != indices.end(); ++iter) {
        cost += lattice.cost(*iter);
        selection.selected_terms.push_back(term_ids[*iter]);
    }
    return std::make_pair(std::move(selection), cost);
}

struct SelectionResult {
    Selection<TermId> selection{};
    std::size_t cost{};
    float next_threshold{};
    std::uint32_t next_docid{};
};

std::ostream& operator<<(std::ostream& out, SelectionResult const& selection_result)
{
    out << "{";
    out << "\tsingle:";
    for (auto i: selection_result.selection.selected_terms) {
        out << ' ' << i;
    }
    out << "\n\tpairs:";
    for (auto i: selection_result.selection.selected_pairs) {
        out << " (" << i.get<0>() << "," << i.get<0>() << ")";
    }
    out << "\n\tcost: " << selection_result.cost << '\n';
    out << "}";
    return out;
}

[[nodiscard]] auto select_intersections(
    QueryRequest const& query, intersection_lattice_type const& lattice, float threshold)
    -> SelectionResult
{
    auto [single_selection, single_cost] = select_essential_single(query, lattice, threshold);
    Selection<TermId> selection;
    auto candidates = lattice.selection_candidates(threshold);
    auto selected = candidates.solve(lattice.costs());
    if (selected.cost >= single_cost) {
        return {std::move(single_selection), single_cost, candidates.next_threshold};
    }
    auto term_ids = query.term_ids();
    for (auto intersection: selected.intersections) {
        if (_mm_popcnt_u32(intersection) == 1) {
            auto idx = __builtin_ctz(intersection);  // TODO: generalize
            selection.selected_terms.push_back(term_ids[idx]);
        } else {
            auto first = __builtin_ctz(intersection);  // TODO: generalize
            auto second = __builtin_ctz(intersection & ~(1U << first));  // TODO: generalize
            selection.selected_pairs.emplace_back(term_ids[first], term_ids[second]);
        }
    }
    return {std::move(selection), selected.cost, candidates.next_threshold};
}

template <typename Cursors>
[[nodiscard]] PISA_ALWAYSINLINE auto calc_cumulative_costs(Cursors&& cursors)
    -> std::vector<std::uint32_t>
{
    std::vector<std::uint32_t> cumulative_costs(cursors.size());
    auto out = cumulative_costs.begin();
    std::uint32_t cost = 0;
    for (auto pos = cursors.begin(); pos != cursors.end(); ++pos) {
        cost += pos->max_score();
        *out++ = cost;
    }
    return cumulative_costs;
}

template <typename CursorIter, typename Fn>
struct MaxScoreState {
    Fn above_threshold;
    std::vector<std::uint32_t> cumulative_costs;
    std::vector<float> upper_bounds;
    CursorIter first_cursor;
    CursorIter first_lookup_cursor;
    std::vector<float>::const_iterator upper_bound_pos;
    std::vector<std::uint32_t>::const_iterator cost_pos;

    template <typename Cursors>
    MaxScoreState(Cursors&& cursors, Fn above_threshold)
        : above_threshold(std::move(above_threshold)),
          cumulative_costs(calc_cumulative_costs(cursors)),
          upper_bounds(calc_upper_bounds(cursors)),
          first_cursor(cursors.begin()),
          first_lookup_cursor(cursors.end()),
          upper_bound_pos(upper_bounds.end()),
          cost_pos(cumulative_costs.end())
    {}

    PISA_ALWAYSINLINE auto update()
    {
        while (first_lookup_cursor != first_cursor && !above_threshold(*std::prev(upper_bound_pos))) {
            --first_lookup_cursor;
            --upper_bound_pos;
            --cost_pos;
        }
    }

    [[nodiscard]] auto cost() const -> std::uint32_t { return *cost_pos; }
};

template <typename Fn, typename Cursors>
[[nodiscard]] PISA_ALWAYSINLINE auto make_maxscore_state(Cursors&& cursors, Fn above_threshold)
    -> MaxScoreState<std::decay_t<decltype(cursors.cbegin())>, Fn>
{
    return MaxScoreState<std::decay_t<decltype(cursors.cbegin())>, Fn>(
        std::forward<Cursors>(cursors), std::move(above_threshold));
}

template <
    typename Cursor,
    typename Payload,
    std::enable_if_t<std::negation<std::is_reference<Payload>>::value, bool> = true>
auto accumulate_single_fn()
{
    return [](Payload& acc, Cursor&& cursor) -> Payload&& {
        acc.score += cursor.score();
        acc.state |= 1U << cursor.term_position();
        return acc;
    };
}

template <
    typename Cursor,
    typename Payload,
    std::enable_if_t<std::negation<std::is_reference<Payload>>::value, bool> = true>
auto accumulate_intersection(Payload& acc, Cursor&& cursor) -> Payload&
{
    acc[cursor.term_position()] = cursor.score();
    return acc;
}
template <
    typename Cursor,
    typename Payload,
    std::enable_if_t<std::negation<std::is_reference<Payload>>::value, bool> = true>
auto accumulate_pair(Payload& acc, Cursor&& cursor) -> Payload&
{
    auto const& score = cursor.score();
    auto const& pos = cursor.term_position();
    acc.accumulate(1U << std::get<0>(pos), std::get<0>(score));
    acc.accumulate(1U << std::get<1>(pos), std::get<1>(score));
    return acc;
}

struct maxscore_inter_opt_query {
    explicit maxscore_inter_opt_query(topk_queue& topk, float pair_cost_scaling = 1.0)
        : m_topk(topk), m_pair_cost_scaling(pair_cost_scaling)
    {}

    auto process_cursors_maxscore() {}

    template <typename EssentialTermCursors, typename EssentialPairCursors, typename LookupCursors>
    auto process_cursors(
        LookupCursors& lookup_cursors,
        EssentialTermCursors&& essential_term_cursors,
        EssentialPairCursors&& essential_pair_cursors,
        gsl::span<TermId const> essential_terms,
        std::size_t term_count,
        std::uint32_t max_docid,
        QueryRequest const& query,
        intersection_lattice_type const& lattice,
        SelectionResult selection_result) -> std::optional<SelectionResult>
    {
        auto accumulate_single = [](auto& acc, auto&& cursor) {
            acc.score += cursor.score();
            acc.state |= 1U << cursor.term_position();
            return acc;
        };
        auto accumulate_pair = [](auto& acc, auto&& cursor) {
            auto const& score = cursor.score();
            auto const& pos = cursor.term_position();
            acc.accumulate(1U << std::get<0>(pos), std::get<0>(score));
            acc.accumulate(1U << std::get<1>(pos), std::get<1>(score));
            return acc;
        };

        Payload initial_payload{};

        auto next_lookup =
            precompute_next_lookup<16>(essential_terms.size(), lookup_cursors.size(), [&] {
                std::vector<std::vector<std::uint32_t>> mapping(term_count);
                for (auto&& cursor: essential_pair_cursors) {
                    auto [left, right] = cursor.term_position();
                    mapping[left].push_back(right);
                    mapping[right].push_back(left);
                }
                return mapping;
            }());

        auto cursor = generic_union_merge(
            initial_payload,
            std::make_tuple(std::move(essential_term_cursors), std::move(essential_pair_cursors)),
            std::make_tuple(accumulate_single, accumulate_pair));

        auto mus = [&] {
            std::vector<float> mus((term_count + 1) * (1U << term_count), 0.0);
            for (auto term_idx = term_count; term_idx + 1 >= 1; term_idx -= 1) {
                for (std::uint32_t j = (1U << term_count) - 1; j + 1 >= 1; j -= 1) {
                    auto state = (term_idx << term_count) + j;
                    auto nt = next_lookup[state];
                    if (nt == -1) {
                        mus[state] = 0.0F;
                    } else {
                        auto a = lookup_cursors[nt - essential_terms.size()].max_score()
                            + mus[((nt + 1) << term_count) + (j | (1 << nt))];
                        auto b = mus[((term_idx + 1) << term_count) + j];
                        mus[state] = std::max(a, b);
                    }
                }
            }
            return mus;
        }();

        State const state_mask = (1U << term_count) - 1;
        auto const initial_state = static_cast<State>(essential_terms.size() << term_count);

        // auto max = selection_result.next_docid == 0 ? max_docid /= 4 : max_docid;
        auto max = max_docid;
        while (cursor.docid() < max) {
            auto const docid = cursor.docid();
            auto const& payload = cursor.payload();

            auto state = payload.state | initial_state;
            auto score = payload.score;

            auto next_idx = next_lookup[state];
            while (PISA_UNLIKELY(next_idx >= 0 && m_topk.would_enter(score + mus[state]))) {
                auto lookup_idx = next_idx - essential_terms.size();
                auto&& lookup_cursor = lookup_cursors[lookup_idx];

                lookup_cursor.next_geq(docid);

                if (lookup_cursor.docid() == docid) {
                    score += lookup_cursor.score();
                    state |= 1U << next_idx;
                }

                state = (state & state_mask) + ((next_idx + 1) << term_count);
                next_idx = next_lookup[state];
            }

            cursor.next();
            m_topk.insert(score, docid);
            // if (m_topk.insert(score, docid) && m_topk.threshold() >=
            // selection_result.next_threshold) {
            //    auto new_selection = select_intersections(query, lattice, m_topk.threshold());
            //    if (new_selection.cost < selection_result.cost) {
            //        new_selection.next_docid = cursor.docid();
            //        return std::make_optional(std::move(new_selection));
            //    }
            //}
        };
        // if (cursor.docid() < max_docid) {
        //    auto new_selection = select_intersections(query, lattice, m_topk.threshold());
        //    new_selection.next_docid = cursor.docid();
        //    return std::make_optional(std::move(new_selection));
        //}
        return std::nullopt;
    }

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
        using lookup_cursor_type =
            NumberedCursor<MaxScoredCursor<typename std::decay_t<Index>::document_enumerator>, TermId>;

        using pair_cursor_type = NumberedCursor<
            PairMaxScoredCursor<typename std::decay_t<PairIndex>::cursor_type>,
            std::array<State, 2>>;

        using inspecting_pair_cursor_type = std::conditional_t<
            std::is_void_v<Inspect>,  //
            pair_cursor_type,
            InspectingCursor<pair_cursor_type, Inspect>>;

        auto const& term_ids = query.term_ids();
        auto const term_count = term_ids.size();

        auto initial_threshold = query.threshold() ? *query.threshold() : 0.0;
        m_topk.set_threshold(initial_threshold);

        // std::uint32_t first_docid = 0;
        // std::uint32_t initial_interval = max_docid / 20 + 1;
        // std::uint32_t interval = initial_interval;
        // std::uint32_t factor = 1;

        // auto calc_last_docid = [&](auto first_docid) {
        //    return max_docid;
        //    // auto interval = (factor++ * initial_interval);
        //    // auto last_docid = first_docid + interval;
        //    // if (last_docid > max_docid) {
        //    //    return max_docid;
        //    //}
        //    // return last_docid;
        //};

        auto lattice = pisa::IntersectionLattice<std::uint16_t>::build(
            query, index, wdata, pair_index, m_pair_cost_scaling);
        auto selection_result =
            std::make_optional(select_intersections(query, lattice, m_topk.threshold()));

        while (selection_result) {
            // auto last_docid = calc_last_docid(first_docid);
            auto const& selection = selection_result->selection;
            if (selection.selected_pairs.empty()) {
                auto process_single = [&](auto algorithm) {
                    auto cursors = inspect_cursors(
                        make_block_max_scored_cursors(index, wdata, scorer, query), inspect);
                    if (auto docid = selection_result->next_docid; docid > 0) {
                        for (auto&& cursor: cursors) {
                            cursor.next_geq(docid);
                        }
                    }
                    algorithm(std::move(cursors), max_docid);
                };
                if (term_ids.size() > 2) {
                    process_single(maxscore_query(m_topk));
                } else {
                    process_single(block_max_wand_query(m_topk));
                }
                // auto max = selection_result->next_docid == 0 ? max_docid / 2 : max_docid;
                // q(std::move(cursors), max);
                // if (max < max_docid) {
                //    selection_result =
                //        std::make_optional(select_intersections(query, lattice,
                //        m_topk.threshold()));
                //    selection_result->next_docid = max;
                //    continue;
                //}
                return;
            }
            auto const essential_terms = selection.selected_terms | to_vector | sort;
            auto const non_essential_terms =
                ranges::views::set_difference(term_ids, essential_terms) | to_vector;

            auto essential_term_cursors = inspect_cursors(
                number_cursors(make_max_scored_cursors(index, wdata, scorer, essential_terms)),
                inspect);
            auto lookup_cursors = inspect_cursors(
                number_cursors(
                    make_max_scored_cursors(index, wdata, scorer, non_essential_terms),
                    non_essential_terms)
                    | sort(std::greater{}, func::max_score{}),
                inspect);

            auto term_position = term_position_function<16>(
                essential_terms, ranges::views::transform(lookup_cursors, [](auto&& cursor) {
                    // Unfortunate naming (probably should be changed); it's actually term ID not
                    // term position
                    return cursor.term_position();
                }));

            std::vector<pair_cursor_type> essential_pair_cursors;
            for (auto [left, right]: selection.selected_pairs) {
                auto pair_id = pair_index.pair_id(left, right);
                State left_pos = term_position(left);
                State right_pos = term_position(right);
                auto cursor = number_cursor(
                    make_max_scored_pair_cursor(
                        pair_index.index(), wdata, *pair_id, scorer, left, right),
                    std::array<State, 2>{left_pos, right_pos});
                essential_pair_cursors.push_back(std::move(cursor));
            }

            if (selection_result->next_docid > 0) {
                for (auto&& cursor: essential_term_cursors) {
                    cursor.next_geq(selection_result->next_docid);
                }
                for (auto&& cursor: essential_pair_cursors) {
                    cursor.next_geq(selection_result->next_docid);
                }
            }

            selection_result = process_cursors(
                lookup_cursors,
                essential_term_cursors,
                std::move(essential_pair_cursors),
                std::move(essential_terms),
                term_ids.size(),
                max_docid,
                query,
                lattice,
                std::move(*selection_result));
            // first_docid = last_docid;
        }
    }

    std::vector<std::pair<float, uint64_t>> const& topk() const { return m_topk.topk(); }

  private:
    topk_queue& m_topk;
    float m_pair_cost_scaling;
};

}  // namespace pisa
