#define CATCH_CONFIG_MAIN
#include "catch2/catch.hpp"

#include "span.hpp"

TEST_CASE("pisa::at", "[span]") {
    std::vector<int> vec{0, 1, 2, 3};
    auto span = std::span<int>{vec.data(), vec.size()};
    REQUIRE(pisa::at(span, 0) == 0);
    REQUIRE(pisa::at(span, 1) == 1);
    REQUIRE(pisa::at(span, 2) == 2);
    REQUIRE(pisa::at(span, 3) == 3);
    REQUIRE_THROWS_AS(pisa::at(span, 4), std::out_of_range);
}

TEST_CASE("operator== for spans", "[span]") {
    std::vector<int> vec1{0, 1, 2, 3};
    auto span1 = std::span<int>(vec1.data(), vec1.size());
    std::vector<int> vec2{0, 1, 2, 3};
    auto span2 = std::span<int>(vec2.data(), vec2.size());
    std::vector<int> vec3{0, 1, 2, -1};
    auto span3 = std::span<int>(vec3.data(), vec3.size());
    REQUIRE(span1 == span2);
    REQUIRE(span1 != span3);
    REQUIRE(span2 != span3);
    REQUIRE(span1 == std::span<int>(vec1.data(), vec1.size()));
}
