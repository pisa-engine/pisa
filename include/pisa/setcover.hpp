#pragma once

#include <vector>

#include <boost/dynamic_bitset.hpp>
#include <gsl/span>

namespace pisa {

template <typename W>
struct Subset {
    boost::dynamic_bitset<std::uint64_t> bits{};
    W weight{};

    /// Constructs a subset directly from its bitset representation.
    Subset(boost::dynamic_bitset<std::uint64_t> bits, W weight)
        : bits(std::move(bits)), weight(weight)
    {}

    /// Constructs a set with only one element out of `cardinality` elements.
    Subset(std::size_t element, std::size_t cardinality, W weight)
        : bits(cardinality, 0), weight(weight)
    {
        bits.set(element);
    }

    /// Constructs a set from the given elements.
    template <typename ElementContainer>
    Subset(ElementContainer&& elements, std::size_t cardinality, W weight)
        : bits(cardinality, 0), weight(weight)
    {
        for (auto&& element: elements) {
            bits.set(element);
        }
    }
};

template <typename W>
struct Result {
    W cost;
    std::vector<std::size_t> selected_indices;
};

/// Given subsets, it returns two bitsets:
/// 1. available: has 1 for any non-empty subset.
/// 2. possible: has 1 for any element in W that can be covered with the given subset.
template <typename W>
auto possible_coverage(gsl::span<Subset<W> const> subsets)
    -> std::pair<boost::dynamic_bitset<std::uint64_t>, boost::dynamic_bitset<std::uint64_t>>
{
    boost::dynamic_bitset<std::uint64_t> available(subsets.size(), 0);
    boost::dynamic_bitset<std::uint64_t> possible(subsets[0].bits.size(), 0);
    available.set();

    for (std::size_t pos = 0; pos < subsets.size(); ++pos) {
        auto&& subset = subsets[pos];
        if (subset.bits.size() != possible.size()) {
            throw std::invalid_argument(
                "All subsets must be represented by bitsets of the same length.");
        }
        possible |= subsets[pos].bits;
        if (!subsets[pos].bits.any()) {
            available.reset(pos);
        }
    }
    return {available, possible};
}

template <typename W>
auto approximate_weighted_set_cover(gsl::span<Subset<W> const> subsets) -> Result<W>
{
    constexpr auto npos = boost::dynamic_bitset<std::uint64_t>::npos;
    if (subsets.empty()) {
        return {};
    }

    auto [available, possible] = possible_coverage(subsets);
    boost::dynamic_bitset<std::uint64_t> covered = ~possible;
    boost::dynamic_bitset<std::uint64_t> selected(subsets.size(), 0);

    W cost{};
    while (!covered.all() && available.any()) {
        auto pos = available.find_first();
        W min_weight = subsets[pos].weight;
        auto min_pos = pos;
        for (pos = available.find_next(pos); pos != npos; pos = available.find_next(pos)) {
            if (auto weight = subsets[pos].weight; weight < min_weight) {
                min_weight = weight;
                min_pos = pos;
            }
        }
        cost += min_weight;
        covered |= subsets[min_pos].bits;
        selected.set(min_pos);
        available.reset(min_pos);
    }
    Result<W> result{cost, std::vector<std::size_t>(selected.count())};
    auto out = result.selected_indices.begin();
    for (auto pos = selected.find_first(); pos != npos; pos = selected.find_next(pos)) {
        *out++ = pos;
    }
    return result;
}

template <typename W>
auto weighted_set_cover(gsl::span<Subset<W> const> subsets) -> Result<W>
{
    constexpr auto npos = boost::dynamic_bitset<std::uint64_t>::npos;
    if (subsets.empty()) {
        return {};
    }

    auto [_, possible] = possible_coverage(subsets);

    auto min_cost = std::numeric_limits<W>::max();
    boost::dynamic_bitset<std::uint64_t> min_solution(subsets.size(), 0);
    for (std::uint64_t solution = 0; solution < (1U << subsets.size()) - 1; solution += 1) {
        boost::dynamic_bitset<std::uint64_t> covered = ~possible;
        boost::dynamic_bitset<std::uint64_t> selected(subsets.size(), solution);
        W cost{};
        for (auto pos = selected.find_first(); pos != npos; pos = selected.find_next(pos)) {
            covered |= subsets[pos].bits;
            cost += subsets[pos].weight;
        }
        if (cost < min_cost && covered.all()) {
            min_cost = cost;
            min_solution = selected;
        }
    }
    Result<W> result{min_cost, std::vector<std::size_t>(min_solution.count())};
    auto out = result.selected_indices.begin();
    for (auto pos = min_solution.find_first(); pos != npos; pos = min_solution.find_next(pos)) {
        *out++ = pos;
    }
    return result;
}

}  // namespace pisa
