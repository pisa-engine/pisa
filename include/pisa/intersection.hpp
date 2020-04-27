#include <bitset>
#include <cstddef>
#include <optional>
#include <variant>
#include <vector>

#include "query/algorithm/and_query.hpp"
#include "query/queries.hpp"
#include "scorer/scorer.hpp"

namespace pisa {

namespace intersection {

    enum class IntersectionType { Query, Combinations };

    /// Mask is set to a static-sized bitset for performance.
    /// Using dynamic bitset slows down a lot, and these operations will be performed on
    /// much shorter queries than the limit allows for. It makes very little sense to do
    /// that on anything longer.
    constexpr std::size_t MAX_QUERY_LEN_EXP = 31;
    constexpr std::size_t MAX_QUERY_LEN = 1 << MAX_QUERY_LEN_EXP;
    using Mask = std::bitset<MAX_QUERY_LEN_EXP>;

    /// Returns a filtered copy of `query` containing only terms indicated by ones in the bit mask.
    [[nodiscard]] inline auto filter(Query const& query, Mask mask) -> Query
    {
        if (query.terms.size() > MAX_QUERY_LEN) {
            throw std::invalid_argument("Queries can be at most 2^32 terms long");
        }
        std::vector<std::uint32_t> terms;
        std::vector<float> weights;
        for (std::size_t bitpos = 0; bitpos < query.terms.size(); ++bitpos) {
            if (((1U << bitpos) & mask.to_ulong()) > 0) {
                terms.push_back(query.terms.at(bitpos));
                if (bitpos < query.term_weights.size()) {
                    weights.push_back(query.term_weights[bitpos]);
                }
            }
        }
        return Query{query.id, terms, weights};
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
        Query const& query,
        std::optional<intersection::Mask> term_mask = std::nullopt) -> Intersection;
};

template <typename Index, typename Wand>
inline auto Intersection::compute(
    Index const& index, Wand const& wand, Query const& query, std::optional<intersection::Mask> term_mask)
    -> Intersection
{
    auto filtered_query = term_mask ? intersection::filter(query, *term_mask) : query;
    scored_and_query retrieve{};
    auto scorer = scorer::from_params(ScorerParams("bm25"), wand);
    auto results = retrieve(make_scored_cursors(index, *scorer, filtered_query), index.num_docs());
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

/// Do `func` for all intersections in a query that have a given maximum number of terms.
/// `Fn` takes `Query` and `Mask`.
template <typename Fn>
auto for_all_subsets(Query const& query, std::optional<std::uint8_t> max_term_count, Fn func)
{
    auto subset_count = 1U << query.terms.size();
    for (auto subset = 1U; subset < subset_count; ++subset) {
        auto mask = intersection::Mask(subset);
        if (!max_term_count || mask.count() <= *max_term_count) {
            func(query, mask);
        }
    }
}

}  // namespace pisa
