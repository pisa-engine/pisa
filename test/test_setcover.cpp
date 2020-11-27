#define CATCH_CONFIG_MAIN
#include "catch2/catch.hpp"

#include <fmt/format.h>
#include <gsl/span>
#include <pisa/setcover.hpp>
#include <rapidcheck.h>

TEST_CASE("Approximate weighted set cover", "[setcover][prop]")
{
    rc::check([](std::vector<std::pair<std::uint8_t, float>> weights) {
        auto sum =
            std::accumulate(weights.begin(), weights.end(), std::uint8_t{}, [](auto acc, auto elem) {
                return acc | elem.first;
            });
        std::vector<pisa::Subset<float>> subsets;
        std::transform(weights.begin(), weights.end(), std::back_inserter(subsets), [](auto entry) {
            return pisa::Subset<float>{
                boost::dynamic_bitset<std::uint64_t>(8, entry.first), std::abs(entry.second)};
        });
        if (sum < std::numeric_limits<std::uint8_t>::max()) {
            subsets.push_back(pisa::Subset<float>{
                boost::dynamic_bitset<std::uint64_t>(8, std::numeric_limits<std::uint8_t>::max()),
                std::numeric_limits<float>::max()});
        }
        auto result =
            pisa::approximate_weighted_set_cover(gsl::span<pisa::Subset<float> const>(subsets));
        sum = std::accumulate(
            result.selected_indices.begin(),
            result.selected_indices.end(),
            std::uint8_t{},
            [&](auto acc, auto idx) {
                return acc | static_cast<std::uint8_t>(subsets[idx].bits.to_ulong());
            });
        REQUIRE(sum == std::numeric_limits<std::uint8_t>::max());
    });
}

TEST_CASE("Exact set cover always better than approx", "[setcover][prop]")
{
    rc::check([](std::vector<std::pair<std::uint8_t, float>> weights) {
        if (weights.size() > 16) {
            return;
        }
        auto sum =
            std::accumulate(weights.begin(), weights.end(), std::uint8_t{}, [](auto acc, auto elem) {
                return acc | elem.first;
            });
        std::vector<pisa::Subset<float>> subsets;
        std::transform(weights.begin(), weights.end(), std::back_inserter(subsets), [](auto entry) {
            return pisa::Subset<float>{
                boost::dynamic_bitset<std::uint64_t>(8, entry.first), std::abs(entry.second)};
        });
        if (sum < std::numeric_limits<std::uint8_t>::max()) {
            subsets.push_back(pisa::Subset<float>{
                boost::dynamic_bitset<std::uint64_t>(8, std::numeric_limits<std::uint8_t>::max()),
                std::numeric_limits<float>::max()});
        }
        auto result =
            pisa::approximate_weighted_set_cover(gsl::span<pisa::Subset<float> const>(subsets));
        sum = std::accumulate(
            result.selected_indices.begin(),
            result.selected_indices.end(),
            std::uint8_t{},
            [&](auto acc, auto idx) {
                return acc | static_cast<std::uint8_t>(subsets[idx].bits.to_ulong());
            });
        REQUIRE(sum == std::numeric_limits<std::uint8_t>::max());
        auto exact_result = pisa::weighted_set_cover(gsl::span<pisa::Subset<float> const>(subsets));
        auto exact_sum = std::accumulate(
            result.selected_indices.begin(),
            result.selected_indices.end(),
            std::uint8_t{},
            [&](auto acc, auto idx) {
                return acc | static_cast<std::uint8_t>(subsets[idx].bits.to_ulong());
            });
        REQUIRE(exact_sum == std::numeric_limits<std::uint8_t>::max());
        REQUIRE(exact_result.cost <= result.cost);
    });
}
