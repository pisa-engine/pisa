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
#include "cursor/cursor_union.hpp"
#include "cursor/inspecting_cursor.hpp"
#include "cursor/lookup_transform.hpp"
#include "cursor/max_scored_cursor.hpp"
#include "cursor/numbered_cursor.hpp"
#include "cursor/union_lookup_join.hpp"
#include "cursor/wand_join.hpp"
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

// template <std::size_t N>
// struct alignas(N < 8 ? 16 : 32) Payload {
//    std::array<float, N> scores{};
//    std::size_t state{0};
//};

struct Payload {
    State state{0};
    float score{0.0};

    PISA_ALWAYSINLINE auto operator+=(Payload const& other) -> Payload&
    {
        // if ((state & other.state) == 0) {
        //    score += other.score;
        //    state |= other.state;
        //}
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

// void horizontal_sum_4(float* lhs, float const* rhs)
//{
//    __m128 const x = _mm_load_ps(lhs);
//    __m128 const y = _mm_load_ps(rhs);
//    __m128 const result = _mm_add_ps(x, y);
//    _mm_store_ps(lhs, result);
//}
//
// void horizontal_sum_8(float* lhs, float const* rhs)
//{
//    __m256 const x = _mm256_load_ps(lhs);
//    __m256 const y = _mm256_load_ps(rhs);
//    __m256 const result = _mm256_add_ps(x, y);
//    _mm256_store_ps(lhs, result);
//}

// template <std::size_t N>
// void accumulate_n(Payload<N>& accumulator, Payload<N> const& payload)
//{
//    auto& acc = accumulator.scores;
//    auto const& score = payload.scores;
//    if constexpr (N == 4) {
//        horizontal_sum_4(&acc[0], &score[0]);
//    } else if constexpr (N == 6) {
//        horizontal_sum_4(&acc[0], &score[0]);
//        acc[4] += score[4];
//        acc[5] += score[5];
//    } else if constexpr (N == 8) {
//        horizontal_sum_8(&acc[0], &score[0]);
//    } else if constexpr (N == 10) {
//        horizontal_sum_8(&acc[0], &score[0]);
//        acc[8] += score[8];
//        acc[9] += score[9];
//    } else if constexpr (N == 12) {
//        horizontal_sum_8(&acc[0], &score[0]);
//        horizontal_sum_4(&acc[8], &score[8]);
//    } else {
//        std::transform(acc.begin(), acc.end(), score.begin(), acc.begin(), std::plus{});
//    }
//    accumulator.state |= payload.state;
//}

//[[nodiscard]] PISA_ALWAYSINLINE auto sum_scores_4(float const* scores) -> float
//{
//    float result;
//    __m128 const x = _mm_load_ps(scores);
//    __m128 const y = _mm_hadd_ps(x, x);
//    __m128 const z = _mm_hadd_ps(y, y);
//    _mm_store_ss(&result, z);
//    return result;
//}
//
//[[nodiscard]] PISA_ALWAYSINLINE auto sum_scores_8(float const* scores) -> float
//{
//    float result;
//    __m128 const a = _mm_load_ps(scores);
//    __m128 const b = _mm_load_ps(std::next(scores, 4));
//    __m128 const x = _mm_hadd_ps(a, b);
//    __m128 const y = _mm_hadd_ps(x, x);
//    __m128 const z = _mm_hadd_ps(y, y);
//    _mm_store_ss(&result, z);
//    return result;
//}
//
// template <std::size_t N>
//[[nodiscard]] PISA_ALWAYSINLINE auto sum_scores(std::array<float, N> const& scores) -> float
//{
//    if constexpr (N == 4) {
//        return sum_scores_4(&scores[0]);
//    } else if (N == 8) {
//        return sum_scores_8(&scores[0]);
//    } else if (N == 12) {
//        return sum_scores_8(&scores[0]) + sum_scores_4(&scores[8]);
//    } else {
//        return std::accumulate(scores.begin(), scores.end(), 0.0F, std::plus{});
//    }
//}

template <std::size_t N, typename R1, typename R2>
[[nodiscard]] PISA_ALWAYSINLINE auto term_position_function(R1&& essential_terms, R2&& lookup_terms)
{
    std::array<TermId, N> ids;
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

struct maxscore_inter_opt_query {
    explicit maxscore_inter_opt_query(topk_queue& topk) : m_topk(topk) {}

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

        auto inspect_cursors = [&](auto&& cursors) {
            if constexpr (std::is_void_v<Inspect>) {
                return std::forward<decltype(cursors)>(cursors);
            } else {
                return ::pisa::inspect_cursors(std::forward<decltype(cursors)>(cursors), *inspect);
            }
        };

        auto is_above_threshold = [this](auto score) { return m_topk.would_enter(score); };

        auto const term_ids = query.term_ids();
        auto const term_count = term_ids.size();

        auto const selection = query.selection();
        if (not selection) {
            throw std::invalid_argument("maxscore_inter_query requires posting list selections");
        }

        auto const essential_terms = selection->selected_terms | to_vector | sort;
        auto const non_essential_terms =
            ranges::views::set_difference(term_ids, essential_terms) | to_vector;

        auto essential_term_cursors =
            number_cursors(make_max_scored_cursors(index, wdata, scorer, essential_terms));
        auto lookup_cursors = inspect_cursors(
            number_cursors(
                make_max_scored_cursors(index, wdata, scorer, non_essential_terms), non_essential_terms)
            | sort(std::greater{}, func::max_score{}));

        auto term_position = term_position_function<16>(
            essential_terms, ranges::views::transform(lookup_cursors, [](auto&& cursor) {
                // Unfortunate naming (probably should be changed); it's actually term ID not
                // term position
                return cursor.term_position();
            }));

        Payload initial_payload{};

        using pair_cursor_type = NumberedCursor<
            PairMaxScoredCursor<typename std::decay_t<PairIndex>::cursor_type>,
            std::array<State, 2>>;

        std::vector<pair_cursor_type> essential_pair_cursors;
        for (auto [left, right]: selection->selected_pairs) {
            auto pair_id = pair_index.pair_id(left, right);
            if (not pair_id) {
                throw std::runtime_error(fmt::format("Pair not found: <{}, {}>", left, right));
            }
            State left_pos = term_position(left);
            State right_pos = term_position(right);
            auto cursor = number_cursor(
                make_max_scored_pair_cursor(pair_index.index(), wdata, *pair_id, scorer, left, right),
                std::array<State, 2>{left_pos, right_pos});  //, left_mask, right_mask});
            essential_pair_cursors.push_back(std::move(cursor));
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
                inspect_cursors(std::move(essential_term_cursors)),
                inspect_cursors(std::move(essential_pair_cursors))),
            std::make_tuple(
                [](auto& acc, auto&& cursor) {
                    acc.score = cursor.score();
                    acc.state |= 1U << cursor.term_position();
                    return acc;
                },
                [](auto& acc, auto&& cursor) {
                    auto const& score = cursor.score();
                    auto const& pos = cursor.term_position();
                    acc.accumulate(1U << std::get<0>(pos), std::get<0>(score));
                    acc.accumulate(1U << std::get<1>(pos), std::get<1>(score));
                    return acc;
                }));

        auto lookup_cursors_upper_bound = std::accumulate(
            lookup_cursors.begin(), lookup_cursors.end(), 0.0F, [](auto acc, auto&& cursor) {
                return acc + cursor.max_score();
            });

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
};

}  // namespace pisa
