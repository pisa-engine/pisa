#define CATCH_CONFIG_MAIN
#include "catch2/catch.hpp"

#include <algorithm>

#include <rapidcheck.h>

#include "algorithm.hpp"

using namespace rc;

const auto genlists = gen::mapcat(gen::inRange<std::size_t>(0, 1000), [](std::size_t length) {
    return gen::pair(
        gen::container<std::vector<int>>(length, gen::arbitrary<int>()),
        gen::container<std::vector<int>>(length, gen::arbitrary<int>()));
});

TEST_CASE("pisa::transform", "[algorithm][prop]")
{
    SECTION("Add 1")
    {
        rc::check([](std::vector<int> vals) {
            auto inc = [](auto val) { return val + 1; };
            std::vector<int> actual;
            pisa::transform(
                pisa::execution::par_unseq, vals.begin(), vals.end(), std::back_inserter(actual), inc);
            std::vector<int> expected;
            std::transform(vals.begin(), vals.end(), std::back_inserter(expected), inc);
            REQUIRE(actual == expected);
        });
    }
    SECTION("Add two sequences")
    {
        rc::check([&]() {
            auto [lhs, rhs] = *genlists;
            std::vector<int> actual;
            pisa::transform(
                pisa::execution::par_unseq,
                lhs.begin(),
                lhs.end(),
                rhs.begin(),
                std::back_inserter(actual),
                std::plus<>{});
            std::vector<int> expected;
            std::transform(
                lhs.begin(), lhs.end(), rhs.begin(), std::back_inserter(expected), std::plus<>{});
            REQUIRE(actual == expected);
        });
    }
}

TEST_CASE("pisa::sort", "[algorithm][prop]")
{
    SECTION("Default sort")
    {
        rc::check([](std::vector<int> vals) {
            std::vector<int> actual = vals;
            pisa::sort(pisa::execution::par_unseq, actual.begin(), actual.end());
            std::vector<int> expected = vals;
            pisa::sort(pisa::execution::par_unseq, expected.begin(), expected.end());
            REQUIRE(actual == expected);
        });
    }
    SECTION("Reverse sort")
    {
        rc::check([](std::vector<int> vals) {
            std::vector<int> actual = vals;
            pisa::sort(pisa::execution::par_unseq, actual.begin(), actual.end(), std::greater<>{});
            std::vector<int> expected = vals;
            std::sort(expected.begin(), expected.end(), std::greater<>{});
            REQUIRE(actual == expected);
        });
    }
}

TEST_CASE("pisa::for_each", "[algorithm][prop]")
{
    rc::check([](std::vector<int> vals) {
        auto inc = [](auto& val) { return val += 1; };
        std::vector<int> actual = vals;
        pisa::for_each(pisa::execution::par_unseq, actual.begin(), actual.end(), inc);
        std::vector<int> expected = vals;
        pisa::for_each(pisa::execution::par_unseq, expected.begin(), expected.end(), inc);
        REQUIRE(actual == expected);
    });
}
