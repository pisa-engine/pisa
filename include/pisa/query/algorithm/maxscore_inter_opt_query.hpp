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

template <typename Index, typename Wand>
[[nodiscard]] auto
select_essential_single(QueryRequest const query, Index&& index, Wand&& wdata, float threshold)
    -> std::pair<Selection<TermId>, std::uint32_t>
{
    auto term_ids = query.term_ids();
    auto term_weights = query.term_weights();
    std::vector<float> max_scores(term_ids.size());
    std::transform(
        term_ids.begin(),
        term_ids.end(),
        term_weights.begin(),
        max_scores.begin(),
        [&](auto term_id, auto term_weight) { return term_weight * wdata.max_term_weight(term_id); });
    std::vector<std::size_t> indices(term_ids.size());
    std::iota(indices.begin(), indices.end(), 0);
    std::sort(indices.begin(), indices.end(), [&max_scores](auto lhs, auto rhs) {
        return max_scores[lhs] < max_scores[rhs];
    });
    float sum = 0.0;
    auto first_essential =
        std::find_if(indices.begin(), indices.end(), [&max_scores, &sum, threshold](auto idx) {
            sum += max_scores[idx];
            return sum > threshold;
        });
    Selection<TermId> selection;
    float cost = 0.0;
    for (auto iter = first_essential; iter != indices.end(); ++iter) {
        cost += wdata.term_posting_count(*iter);
        selection.selected_terms.push_back(term_ids[*iter]);
    }
    return std::make_pair(std::move(selection), cost);
}

template <typename Index, typename Wand, typename PairIndex>
[[nodiscard]] auto select_intersections(
    QueryRequest const query, Index&& index, Wand&& wdata, PairIndex&& pair_index, float threshold)
    -> Selection<TermId>
{
    auto [single_selection, single_cost] = select_essential_single(query, index, wdata, threshold);
    Selection<TermId> selection;
    // std::cerr << "t: " << threshold << '\n';
    auto lattice = pisa::IntersectionLattice<std::uint16_t>::build(query, index, wdata, pair_index);
    auto candidates = lattice.selection_candidates(threshold);
    auto selected = candidates.solve(lattice.costs());
    if (selected.cost >= single_cost) {
        return single_selection;
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
    return selection;
}

struct maxscore_inter_opt_query {
    explicit maxscore_inter_opt_query(topk_queue& topk, bool dynamic_intersections = false)
        : m_topk(topk), m_dynamic_intersections(dynamic_intersections)
    {}

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

        auto const& term_ids = query.term_ids();
        auto const term_count = term_ids.size();

        auto initial_threshold = query.threshold() ? *query.threshold() : 0.0;
        m_topk.set_threshold(initial_threshold);

        // auto print_sel = [](auto selection, auto caption) {
        //    std::cerr << caption << ":";
        //    for (auto&& inter: selection.selected_terms) {
        //        std::cerr << " " << inter;
        //    }
        //    std::cerr << " | ";
        //    for (auto&& inter: selection.selected_pairs) {
        //        std::cerr << " (" << std::get<0>(inter) << "," << std::get<1>(inter) << ")";
        //    }
        //    std::cerr << "\n";
        //};
        // print_sel(*query.selection(), "Loaded");
        auto selection = select_intersections(query, index, wdata, pair_index, initial_threshold);
        // print_sel(selection, "Computed");
        if (selection.selected_pairs.empty()) {
            maxscore_query q(m_topk);
            q(make_block_max_scored_cursors(index, wdata, scorer, query), index.num_docs());
            return;
        }

        // auto const selection = query.selection();
        // if (not selection) {
        //    throw std::invalid_argument("maxscore_inter_query requires posting list selections");
        //}

        auto const essential_terms = selection.selected_terms | to_vector | sort;
        auto const non_essential_terms =
            ranges::views::set_difference(term_ids, essential_terms) | to_vector;

        auto essential_term_cursors =
            number_cursors(make_max_scored_cursors(index, wdata, scorer, essential_terms));
        auto lookup_cursors = inspect_cursors(
            number_cursors(
                make_max_scored_cursors(index, wdata, scorer, non_essential_terms), non_essential_terms)
                | sort(std::greater{}, func::max_score{}),
            inspect);

        auto term_position = term_position_function<16>(
            essential_terms, ranges::views::transform(lookup_cursors, [](auto&& cursor) {
                // Unfortunate naming (probably should be changed); it's actually term ID not
                // term position
                return cursor.term_position();
            }));

        Payload initial_payload{};

        auto accumulate_single = [](auto& acc, auto&& cursor) {
            acc.score += cursor.score();
            acc.state |= 1U << cursor.term_position();
            return acc;
        };
        auto accumulate_intersection = [](auto& acc, auto&& cursor) {
            acc[cursor.term_position()] = cursor.score();
            return acc;
        };
        auto accumulate_pair = [](auto& acc, auto&& cursor) {
            auto const& score = cursor.score();
            auto const& pos = cursor.term_position();
            acc.accumulate(1U << std::get<0>(pos), std::get<0>(score));
            acc.accumulate(1U << std::get<1>(pos), std::get<1>(score));
            return acc;
        };

        using pair_cursor_type = NumberedCursor<
            PairMaxScoredCursor<typename std::decay_t<PairIndex>::cursor_type>,
            std::array<State, 2>>;

        using intersection_type = NumberedCursor<
            CursorIntersection<
                std::decay_t<decltype(number_cursors(make_max_scored_cursors(
                    index, wdata, scorer, std::declval<std::vector<TermId>>())))>,
                std::array<float, 2>,
                decltype(accumulate_intersection)>,
            std::array<State, 2>>;

        std::vector<pair_cursor_type> essential_pair_cursors;
        std::vector<intersection_type> essential_intersections;
        for (auto [left, right]: selection.selected_pairs) {
            auto pair_id = pair_index.pair_id(left, right);
            State left_pos = term_position(left);
            State right_pos = term_position(right);
            if (m_dynamic_intersections || not pair_id) {
                if (not m_dynamic_intersections) {
                    throw std::runtime_error(fmt::format("Pair not found: <{}, {}>", left, right));
                }
                auto cursors = number_cursors(make_max_scored_cursors(
                    index, wdata, scorer, std::vector<TermId>{left, right}));
                essential_intersections.push_back(number_cursor(
                    intersect(std::move(cursors), std::array<float, 2>{}, accumulate_intersection),
                    std::array<State, 2>{left_pos, right_pos}));
            } else {
                auto cursor = number_cursor(
                    make_max_scored_pair_cursor(
                        pair_index.index(), wdata, *pair_id, scorer, left, right),
                    std::array<State, 2>{left_pos, right_pos});
                essential_pair_cursors.push_back(std::move(cursor));
            }
        }

        auto next_lookup =
            precompute_next_lookup<16>(essential_terms.size(), lookup_cursors.size(), [&] {
                std::vector<std::vector<std::uint32_t>> mapping(term_ids.size());
                for (auto&& cursor: essential_pair_cursors) {
                    auto [left, right] = cursor.term_position();
                    mapping[left].push_back(right);
                    mapping[right].push_back(left);
                }
                return mapping;
            }());

        auto cursor = generic_union_merge(
            initial_payload,
            std::make_tuple(
                inspect_cursors(std::move(essential_term_cursors), inspect),
                inspect_cursors(std::move(essential_pair_cursors), inspect),
                inspect_cursors(std::move(essential_intersections), inspect)),
            std::make_tuple(accumulate_single, accumulate_pair, accumulate_pair));

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

        while (cursor.docid() < max_docid) {
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

            m_topk.insert(score, docid);
            cursor.next();
        };
    }

    std::vector<std::pair<float, uint64_t>> const& topk() const { return m_topk.topk(); }

  private:
    topk_queue& m_topk;
    bool m_dynamic_intersections;
};

}  // namespace pisa
