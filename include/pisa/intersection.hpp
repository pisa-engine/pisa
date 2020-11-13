#include <algorithm>
#include <bitset>
#include <cstddef>
#include <optional>
#include <variant>
#include <vector>

#include <gsl/span>

#include "binary_index.hpp"
#include "cursor/scored_cursor.hpp"
#include "query.hpp"
#include "query/algorithm/and_query.hpp"
#include "scorer/scorer.hpp"
#include "setcover.hpp"
#include "util/intrinsics.hpp"

namespace pisa {

namespace intersection {

    enum class IntersectionType { Query, Combinations, ExistingCombinations };

    /// Mask is set to a static-sized bitset for performance.
    /// Using dynamic bitset slows down a lot, and these operations will be performed on
    /// much shorter queries than the limit allows for. It makes very little sense to do
    /// that on anything longer.
    constexpr std::size_t MAX_QUERY_LEN_EXP = 31;
    constexpr std::size_t MAX_QUERY_LEN = 1 << MAX_QUERY_LEN_EXP;
    using Mask = std::bitset<MAX_QUERY_LEN_EXP>;

    /// Returns a filtered copy of `query` containing only terms indicated by ones in the bit mask.
    [[nodiscard]] inline auto filter(QueryContainer const& query, Mask mask) -> QueryContainer
    {
        std::vector<std::size_t> positions;
        for (std::size_t bitpos = 0; mask.any(); ++bitpos) {
            if (mask.test(bitpos)) {
                positions.push_back(bitpos);
                mask.reset(bitpos);
            }
        }
        QueryContainer filtered_query(query);
        filtered_query.filter_terms(positions);
        return filtered_query;
    }
}  // namespace intersection

/// Represents information about an intersection of one or more terms of a query.
struct Intersection {
    /// Number of postings in the intersection.
    std::size_t length;
    /// Maximum partial score in the intersection.
    float max_score;

    template <typename Index, typename Wand>
    inline static auto compute(
        Index const& index,
        Wand const& wand,
        QueryContainer const& query,
        ScorerParams const& scorer_params,
        std::optional<intersection::Mask> term_mask = std::nullopt) -> Intersection;

    template <typename Index, typename Wand, typename PairIndex>
    inline static auto compute(
        Index const& index,
        Wand const& wand,
        QueryContainer const& query,
        ScorerParams const& scorer_params,
        intersection::Mask term_mask,
        PairIndex const& pair_index) -> Intersection;
};

template <typename Index, typename Wand>
inline auto Intersection::compute(
    Index const& index,
    Wand const& wand,
    QueryContainer const& query,
    ScorerParams const& scorer_params,
    std::optional<intersection::Mask> term_mask) -> Intersection
{
    auto filtered_query = term_mask ? intersection::filter(query, *term_mask) : query;
    scored_and_query retrieve{};
    auto scorer = scorer::from_params(scorer_params, wand);
    auto results = retrieve(
        make_scored_cursors(index, *scorer, filtered_query.query(query::unlimited)),
        index.num_docs());
    auto max_element = [&](auto const& vec) -> float {
        auto order = [](auto const& lhs, auto const& rhs) { return lhs.second < rhs.second; };
        if (auto pos = std::max_element(results.begin(), results.end(), order); pos != results.end()) {
            return pos->second;
        }
        return 0.0;
    };
    float max_score = max_element(results);
    return Intersection{results.size(), max_score};
}

template <typename Index, typename Wand, typename PairIndex>
inline auto Intersection::compute(
    Index const& index,
    Wand const& wand,
    QueryContainer const& query,
    ScorerParams const& scorer_params,
    intersection::Mask term_mask,
    PairIndex const& pair_index) -> Intersection
{
    auto scorer = scorer::from_params(scorer_params, wand);
    auto filtered_query = intersection::filter(query, term_mask);
    auto request = filtered_query.query(query::unlimited);
    auto term_ids = request.term_ids();

    if (term_ids.size() == 2) {
        auto pair_id = pair_index.pair_id(term_ids[0], term_ids[1]);
        if (not pair_id) {
            return Intersection{0, 0.0};
        }
        auto cursor = PairScoredCursor<std::decay_t<decltype(pair_index.index()[*pair_id])>>(
            pair_index.index()[*pair_id],
            scorer->term_scorer(term_ids[0]),
            scorer->term_scorer(term_ids[1]),
            1.0);
        auto size = cursor.size();
        float max_score = 0.0;
        while (cursor.docid() < cursor.universe()) {
            auto scores = cursor.score();
            auto score = std::get<0>(scores) + std::get<1>(scores);
            if (score > max_score) {
                max_score = score;
            }
            cursor.next();
        }
        return Intersection{size, max_score};
    }
    if (term_ids.size() == 1) {
        auto cursor = ScoredCursor<std::decay_t<decltype(index[0])>>(
            index[term_ids[0]], scorer->term_scorer(term_ids[0]), 1.0);
        auto size = cursor.size();
        float max_score = 0.0;
        while (cursor.docid() < cursor.universe()) {
            if (auto score = cursor.score(); score > max_score) {
                max_score = score;
            }
            cursor.next();
        }
        return Intersection{size, max_score};
    }
    return Intersection{0, 0.0};
}

/// Do `func` for all intersections in a query that have a given maximum number of terms.
/// `Fn` takes `QueryContainer` and `Mask`.
template <typename Fn>
auto for_all_subsets(QueryContainer const& query, std::optional<int> max_term_count, Fn func)
{
    auto subset_count = 1U << query.term_ids()->size();
    for (auto subset = 1U; subset < subset_count; ++subset) {
        auto mask = intersection::Mask(subset);
        if (!max_term_count || (mask.count() <= *max_term_count)) {
            func(query, mask);
        }
    }
}

/// The result of intersection selection.
template <typename S>
struct Selected {
    /// The solution in form of bitmask subset representations.
    std::vector<S> intersections;
    /// Final cost of the seleciton.
    std::size_t cost;
};

/// Candidates for intersection selection returned from the query's intersection lattice.
template <typename S>
struct SelectionCandidtes {
    std::vector<S> subsets;
    std::vector<S> elements;

    /// Return selected index structures. Each structure is represented as a bitmask, so these still
    /// need to be translated into query term IDs.
    [[nodiscard]] auto solve(std::array<std::uint32_t, std::numeric_limits<S>::max() + 1> const& costs)
        -> Selected<S>
    {
        std::vector<Subset<std::size_t>> input;
        std::vector<S> intersections;
        for (auto sub: this->subsets) {
            boost::dynamic_bitset<std::uint64_t> bits(elements.size(), 0);
            for (std::size_t idx = 0; idx < elements.size(); ++idx) {
                auto mask = elements[idx];
                if ((mask & sub) == sub) {
                    bits.set(idx);
                }
            }
            if (!bits.empty()) {
                input.emplace_back(std::move(bits), costs[sub]);
                intersections.push_back(sub);
            }
        }
        auto result = approximate_weighted_set_cover(gsl::span<Subset<std::size_t> const>(input));
        Selected<S> selected;
        std::transform(
            result.selected_indices.begin(),
            result.selected_indices.end(),
            std::back_inserter(selected.intersections),
            [&intersections](auto idx) { return intersections[idx]; });
        selected.cost = result.cost;
        return selected;
    }
};

template <class InputIt, class OutputIt>
constexpr  // since C++20
    OutputIt
    partial_sum(InputIt first, InputIt last, OutputIt d_first)
{
    if (first == last) {
        return d_first;
    }

    typename std::iterator_traits<InputIt>::value_type sum = *first;
    *d_first = sum;

    while (++first != last) {
        sum = std::move(sum) + *first;  // std::move since C++20
        *++d_first = sum;
    }
    return ++d_first;
}

class PascalTriangle {
    std::array<std::uint32_t, 153> values{};
    std::array<std::uint32_t, 153> prefix_sums{};

    [[nodiscard]] constexpr static auto offset(std::size_t row) -> std::size_t
    {
        switch (row) {
        case 0: return 0;
        case 1: return 1;
        case 2: return 3;
        case 3: return 6;
        case 4: return 10;
        case 5: return 15;
        case 6: return 21;
        case 7: return 28;
        case 8: return 36;
        case 9: return 45;
        case 10: return 55;
        case 11: return 66;
        case 12: return 78;
        case 13: return 91;
        case 14: return 105;
        case 15: return 120;
        case 16: return 136;
        case 17: return 153;
        default: throw std::invalid_argument("Row must be up to 17");
        }
    }

  public:
    [[nodiscard]] constexpr auto intersection_counts(std::size_t num_terms) const
        -> gsl::span<std::uint32_t const>
    {
        return gsl::span<std::uint32_t const>(
            std::next(values.begin(), offset(num_terms) + 1), num_terms);
    }
    [[nodiscard]] constexpr auto intersection_count_partial_sum(std::size_t num_terms) const
        -> gsl::span<std::uint32_t const>
    {
        return gsl::span<std::uint32_t const>(
            std::next(prefix_sums.begin(), offset(num_terms) + 1), num_terms);
    }

    constexpr static auto build() -> PascalTriangle
    {
        PascalTriangle triangle;
        for (std::size_t row = 1; row <= 16; row += 1) {
            triangle.values[offset(row)] = 1;
            triangle.values[offset(row) - 1] = 1;
        }
        for (std::size_t row = 2; row <= 16; row += 1) {
            auto start = offset(row) + 1;
            for (std::size_t pos = start; pos < start + row - 1; ++pos) {
                triangle.values[pos] = triangle.values[pos - row] + triangle.values[pos - row - 1];
            }
        }
        for (std::size_t num_terms = 2; num_terms <= 16; num_terms += 1) {
            auto counts = triangle.intersection_counts(num_terms);
            auto out = std::next(triangle.prefix_sums.begin(), offset(num_terms) + 1);
            *out++ = 0;
            pisa::partial_sum(counts.begin(), std::prev(counts.end()), out);
        }
        return triangle;
    }
};

constexpr PascalTriangle PASCAL_TRIANGLE = PascalTriangle::build();

/// Representation of the intersection lattice of a query.
///
/// An intersection lattice is a structure used to determined which posting lists
/// (including cached intersections) are essential (as in MaxScore) and to select
/// the optimal set of lists that minimize cost.
///
/// An example of a lattice is:
///
///     A   B   C   D
///
///   AB AC AD BC BD CD
///
///    ABC ABD ACD BCD
///
/// where A, B, C, and D represent single term posting lists, and XYZ is an intersection of
/// X, Y, and Z.
///
/// Each node in the lattice represents a **result class** that contains all documents in that
/// posting list. Each result class has a max score value associated with it.
/// For a query with an estimated score threshold T, a each result class whose max score is
/// higher than T must be **covered**.
/// A result class is covered if its node is selected or if a node that is a subset of its terms
/// is selected.
/// E.g., A covers any class containing A, while AB covers any class that contain BOTH A and B.
template <typename S>
class IntersectionLattice {
    static_assert(
        std::is_integral_v<S> && std::is_unsigned_v<S>,
        "Subset representation must be an unsigned integral type.");

    static_assert(sizeof(S) <= 2, "Subset representation must be at most 16 bit long.");

    constexpr static int max_query_length = std::numeric_limits<S>::digits;
    constexpr static auto max_subset_count =
        static_cast<std::size_t>(std::numeric_limits<S>::max()) + 1;

    void cover(std::bitset<max_subset_count>& covered, S mask, gsl::span<S const> nodes) const
    {
        covered.set(mask);
        auto arity = _mm_popcnt_u32(mask);
        if (arity < query_length()) {
            std::size_t offset =
                PASCAL_TRIANGLE.intersection_count_partial_sum(query_length())[arity];
            std::for_each(std::next(nodes.begin(), offset), nodes.end(), [&](auto subset) {
                if ((subset & mask) == mask) {
                    covered.set(subset);
                }
            });
        }
    }

  public:
    auto layered_nodes() const
    {
        std::array<S, max_subset_count> nodes;
        auto cap = static_cast<std::uint64_t>(1) << query_length();
        std::iota(nodes.begin(), std::next(nodes.begin(), cap), 1);
        std::sort(nodes.begin(), std::next(nodes.begin(), cap - 1), [](auto lhs, auto rhs) {
            // TODO: fix perf
            return std::make_pair(_mm_popcnt_u32(lhs), lhs)
                < std::make_pair(_mm_popcnt_u32(rhs), rhs);
        });
        return nodes;
    }

    /// Builds an intersection lattice for the given query request.
    /// All the necessary data is pulled from the given index objects.
    ///
    /// # Exceptions
    ///
    /// This function will throw `std::invalid_argument` if the query is longer than the number of
    /// bits contained in the subset representation type `S`.
    template <typename Index, typename Wand, typename PairIndex>
    [[nodiscard]] static auto build(
        QueryRequest const query,
        Index&& index,
        Wand&& wdata,
        PairIndex&& pair_index,
        float pair_cost_scaling = 1.0) -> IntersectionLattice
    {
        auto term_ids = query.term_ids();
        auto term_weights = query.term_weights();

        if (term_ids.size() > max_query_length) {
            throw std::invalid_argument("Query too long for the subset type.");
        }

        IntersectionLattice lattice;

        for (std::size_t pos = 0; pos < term_ids.size(); ++pos) {
            auto mask = S{1} << pos;
            lattice.m_single_term_lists.push_back(mask);
            auto term_id = term_ids[pos];
            auto term_weight = term_weights[pos];
            lattice.m_score_bounds[mask] = term_weight * wdata.max_term_weight(term_id);
            lattice.m_costs[mask] = wdata.term_posting_count(term_id);
        }

        for (std::size_t first = 0; first < term_ids.size(); ++first) {
            for (std::size_t second = first + 1; second < term_ids.size(); ++second) {
                auto mask = (S{1} << first) | (S{1} << second);
                if (auto pair_id = pair_index.pair_id(term_ids[first], term_ids[second]);
                    pair_id.has_value()) {
                    lattice.m_pair_intersections.push_back(mask);
                    /* lattice.m_costs[mask] = static_cast<std::uint32_t>( */
                    /*     static_cast<float>(pair_index.pair_posting_count(*pair_id)) */
                    /*     * pair_cost_scaling); */
                    lattice.m_costs[mask] = pair_index.pair_posting_count(*pair_id);
                    // TODO: Use actual max score.
                    lattice.m_score_bounds[mask] = lattice.m_score_bounds[static_cast<S>(1) << first]
                        + lattice.m_score_bounds[static_cast<S>(1) << second];
                } else {
                    lattice.m_costs[mask] = std::numeric_limits<std::uint32_t>::max();
                    lattice.m_score_bounds[mask] = lattice.m_score_bounds[static_cast<S>(1) << first]
                        + lattice.m_score_bounds[static_cast<S>(1) << second];
                }
            }
        }

        lattice.calc_remaining_score_bounds();
        return lattice;
    }

    void calc_remaining_score_bounds()
    {
        for (std::uint32_t subset = 1; subset < (1U << query_length()); ++subset) {
            if (m_score_bounds[subset] == 0) {
                auto mask = subset;
                while (mask > 0) {
                    auto idx = __builtin_ctz(mask);  // TODO: generalize
                    S single = static_cast<S>(1) << idx;
                    m_score_bounds[subset] += m_score_bounds[single];
                    mask &= ~single;
                }
            }
        }
    }

    /// Return the number of terms in the query this lattice was constructed for.
    [[nodiscard]] auto query_length() const noexcept -> std::size_t
    {
        return m_single_term_lists.size();
    }

    /// Finds a set of candidates to be selected.
    ///
    /// Some intersections can be discarded right away to reduce the size of the optimization
    /// problem. For example, if we can determine that A must be selected, then selecting
    /// any intersection containing A is pointless because A covers them already.
    ///
    /// Selection candidates can be divided into two groups:
    /// - leaves: nodes that must be covered because their max score exceeds the threshold,
    ///           but none of the nodes that cover them exceed threshold.
    /// - inner nodes: nodes that are not leaves and not covered by any leaves.
    ///
    /// This candidate set can be turned into a set cover problem where the leaves are the
    /// set of elements, and all candidates are subsets (including the leaves, which are
    /// singletons).
    [[nodiscard]] auto selection_candidates(float threshold) const noexcept -> SelectionCandidtes<S>
    {
        SelectionCandidtes<S> candidates;
        auto& subsets = candidates.subsets;
        auto& elements = candidates.elements;
        S covered_single{0};
        std::bitset<max_subset_count> covered;
        std::bitset<max_query_length * max_query_length> considered_pairs;
        auto pair_idx = [len = query_length()](auto mask) {
            auto first = __builtin_ctz(mask);  // TODO: generalize
            auto second = __builtin_ctz(mask & ~(1U << first));  // TODO: generalize
            return first * len + second;
        };
        auto lnodes = layered_nodes();
        gsl::span<S const> nodes(lnodes.begin(), 1U << query_length());
        for (auto mask: m_single_term_lists) {
            if (score_bound(mask) >= threshold) {
                covered_single |= mask;
                cover(covered, mask, nodes);
                elements.push_back(mask);
            }
            subsets.push_back(mask);
        }
        for (auto mask: m_pair_intersections) {
            if (not covered.test(mask)) {
                if (score_bound(mask) >= threshold) {
                    elements.push_back(mask);
                    cover(covered, mask, nodes);
                }
                subsets.push_back(mask);
                considered_pairs.set(pair_idx(mask));
            }
        }
        std::for_each(std::next(nodes.begin(), query_length()), nodes.end(), [&](auto subset) {
            if (_mm_popcnt_u32(subset) > 1 && not covered.test(subset)) {  // TODO: generalize
                if (_mm_popcnt_u32(subset) == 2 && considered_pairs.test(pair_idx(subset))) {
                    return;
                }
                if (score_bound(subset) >= threshold) {
                    cover(covered, subset, nodes);
                    elements.push_back(subset);
                }
            }
        });
        return candidates;
    }

    [[nodiscard]] auto single_term_lists() const noexcept -> gsl::span<S const>
    {
        return gsl::span<S const>(m_single_term_lists);
    }

    [[nodiscard]] auto pair_intersections() const noexcept -> gsl::span<S const>
    {
        return gsl::span<S const>(m_pair_intersections);
    }

    [[nodiscard]] auto const& costs() const { return m_costs; }
    [[nodiscard]] auto cost(S subset) const -> std::uint32_t { return m_costs[subset]; }
    [[nodiscard]] auto score_bound(S subset) const -> float { return m_score_bounds[subset]; }

    IntersectionLattice(
        std::vector<S> single_term_lists,
        std::vector<S> pair_intersections,
        std::array<std::uint32_t, max_subset_count> costs,
        std::array<float, max_subset_count> score_bounds)
        : m_single_term_lists(std::move(single_term_lists)),
          m_pair_intersections(std::move(pair_intersections)),
          m_costs(std::move(costs)),
          m_score_bounds(std::move(score_bounds))
    {}

  private:
    constexpr IntersectionLattice() { m_costs.fill(std::numeric_limits<std::uint32_t>::max()); }
    std::vector<S> m_single_term_lists{};
    std::vector<S> m_pair_intersections{};
    std::array<std::uint32_t, max_subset_count> m_costs{};
    std::array<float, max_subset_count> m_score_bounds{};
};

}  // namespace pisa
