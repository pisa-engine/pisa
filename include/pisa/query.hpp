// Copyright 2024 PISA Developers
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <algorithm>
#include <optional>
#include <string>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <vector>

#include "type_alias.hpp"

namespace pisa {

/** Term ID along with its weight.
 *
 * Typically, a weight would be equal to the number of occurrences of the term in a query.
 * Partial scores coming from this term will be multiplied by this weight.
 */
struct WeightedTerm {
    TermId id;
    Score weight;

    /** Tuple conversion for structured bindings support.
     *
     * ```
     * WeightedTerm wt{0, 1.0};
     * auto [term_id, weight] = weighted_term;
     * ```
     */
    [[nodiscard]] explicit constexpr operator std::pair<TermId, Score>() const noexcept;
};

[[nodiscard]] inline auto operator==(WeightedTerm const& lhs, WeightedTerm const& rhs) -> bool {
    return std::tie(lhs.id, lhs.weight) == std::tie(rhs.id, rhs.weight);
}

namespace query {

    /**
     * Tells `Query` how to process the terms passed to the constructor.
     *
     * By default, duplicate terms will be removed, and the weight of each term will be equal to
     * the number of occurrences of that term in the query. Furthermore, the order of the terms
     * will be preserved (if there are duplicates, the term will be at the position of its first
     * occurrence.
     *
     * This policy can be modified with the following options:
     *  - `keep_duplicates`: duplicates will be preserved, each with weight 1.0
     *    (inefficient -- see below);
     *  - `unweighted`: forces each weight to be 1.0 even if duplicates are removed;
     *  - `sort`: sorts terms by ID.
     *
     * !! Note that `keep_duplicates` is very inefficient if used for retrieval because some
     * posting: lists will have to be traversed multiple times if duplicate terms exist. Do not use
     * it unless you know exactly what you are doing (e.g. if you use Query outside of the standard
     * query processing and you rely on duplicates).
     *
     * Policies can be combined similar to bitsets. For example, `unweighted | sort` will both
     * force unit weights and sort the terms.
     */
    struct TermPolicy {
        std::uint32_t policy;

        /** Checks if this policy contains the other policy. */
        [[nodiscard]] constexpr auto contains(TermPolicy const& other) const noexcept -> bool;
    };

    /** Merges two policies; the resulting policy will policies from both arguments. */
    [[nodiscard]] auto operator|(TermPolicy lhs, TermPolicy rhs) noexcept -> TermPolicy;

    /** Duplicates are removed and weights are equal to number of occurrences of each term in the
     * query. Terms are not sorted. */
    static constexpr TermPolicy default_policy = {0b000};

    /** Keep duplicates. */
    static constexpr TermPolicy keep_duplicates = {0b001};

    /** Use weight 1.0 for each resulting term. */
    static constexpr TermPolicy unweighted = {0b010};

    /** Sort by term ID. */
    static constexpr TermPolicy sort = {0b100};

}  // namespace query

/**
 * A query issued to the system.
 */
class Query {
    std::optional<std::string> m_id{};
    std::vector<WeightedTerm> m_terms{};

    void postprocess(query::TermPolicy policy);

  public:
    /** Constructs a query with the given ID from the terms and weights given by the iterators.
     */
    template <typename TermIterator, typename WeightIterator>
    Query(
        std::optional<std::string> id,
        TermIterator first_term,
        TermIterator last_term,
        WeightIterator first_weight,
        query::TermPolicy policy = query::default_policy
    )
        : m_id(std::move(id)) {
        using value_type = typename std::iterator_traits<TermIterator>::value_type;
        static_assert(std::is_constructible_v<TermId, value_type>);
        std::transform(
            first_term, last_term, first_weight, std::back_inserter(m_terms), [](auto id, auto weight) {
                return WeightedTerm{id, weight};
            }
        );
        postprocess(policy);
    }

    /** Constructs a query with the given ID from the terms given by the iterators. */
    template <typename Iterator>
    Query(
        std::optional<std::string> id,
        Iterator first,
        Iterator last,
        query::TermPolicy policy = query::default_policy
    )
        : m_id(std::move(id)) {
        using value_type = typename std::iterator_traits<Iterator>::value_type;
        static_assert(std::is_constructible_v<TermId, value_type>);
        std::transform(first, last, std::back_inserter(m_terms), [](auto id) {
            return WeightedTerm{id, 1.0};
        });
        postprocess(policy);
    }

    /** Constructs a query with the given ID from the terms and weights passed as collections.
     */
    template <typename Terms, typename Weights>
    explicit Query(
        std::optional<std::string> id,
        Terms const& terms,
        Weights const& weights,
        query::TermPolicy policy = {0}
    )
        : Query(std::move(id), terms.begin(), terms.end(), weights.begin(), policy) {}

    /** Constructs a query with the given ID from the terms passed as a collection. */
    template <typename Collection>
    explicit Query(
        std::optional<std::string> id,
        Collection const& terms,
        query::TermPolicy policy = query::default_policy
    )
        : Query(std::move(id), terms.begin(), terms.end(), policy) {}

    /** Returns the ID of the query if defined. */
    [[nodiscard]] auto id() const noexcept -> std::optional<std::string_view>;

    /** Returns the reference to all weighted terms. */
    [[nodiscard]] auto terms() const noexcept -> std::vector<WeightedTerm> const&;
};

}  // namespace pisa
