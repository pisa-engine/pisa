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

}  // namespace pisa
