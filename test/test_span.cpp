// Copyright 2024 PISA developers
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

#define CATCH_CONFIG_MAIN
#include "catch2/catch.hpp"

#include <algorithm>
#include <string_view>

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

TEST_CASE("pisa::subspan", "[span]") {
    std::vector<int> vec{0, 1, 2, 3};
    auto span = std::span<int>{vec.data(), vec.size()};
    REQUIRE(pisa::subspan_or_throw(span, 0, 0) == std::span<int>(vec.data(), 0));
    REQUIRE(pisa::subspan_or_throw(span, 0, 1) == std::span<int>(vec.data(), 1));
    REQUIRE(pisa::subspan_or_throw(span, 1, 0) == std::span<int>(vec.data() + 1, 0));
    REQUIRE(pisa::subspan_or_throw(span, 0, 4) == std::span<int>(vec.data(), 4));
    REQUIRE(pisa::subspan_or_throw(span, 1, 3) == std::span<int>(vec.data() + 1, 3));
    REQUIRE(pisa::subspan_or_throw(span, 0, 3) == std::span<int>(vec.data(), 3));
    REQUIRE(pisa::subspan_or_throw(span, 2, 2) == std::span<int>(vec.data() + 2, 2));
    REQUIRE(pisa::subspan_or_throw(span, 3, 1) == std::span<int>(vec.data() + 3, 1));
    REQUIRE(pisa::subspan_or_throw(span, 4, 0) == std::span<int>(vec.data() + 4, 0));
    REQUIRE_THROWS_AS(pisa::subspan_or_throw(span, 0, 6), std::out_of_range);
    REQUIRE_THROWS_AS(pisa::subspan_or_throw(span, 0, 5), std::out_of_range);
    REQUIRE_THROWS_AS(pisa::subspan_or_throw(span, 1, 4), std::out_of_range);
    REQUIRE_THROWS_AS(pisa::subspan_or_throw(span, 2, 3), std::out_of_range);
    REQUIRE_THROWS_AS(pisa::subspan_or_throw(span, 3, 2), std::out_of_range);
    REQUIRE_THROWS_AS(pisa::subspan_or_throw(span, 4, 1), std::out_of_range);
    REQUIRE_THROWS_AS(pisa::subspan_or_throw(span, 5, 0), std::out_of_range);
    REQUIRE_THROWS_AS(pisa::subspan_or_throw(span, 5, 1), std::out_of_range);
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

TEST_CASE("lex_lt", "[span]") {
    std::string_view aardvark = "aardvark";
    std::string_view dog = "dog";
    std::string_view zebra = "zebra";
    std::string_view empty = "";

    REQUIRE_FALSE(pisa::lex_lt(std::span(aardvark), std::span(aardvark)));
    REQUIRE(pisa::lex_lt(std::span(aardvark), std::span(dog)));
    REQUIRE(pisa::lex_lt(std::span(aardvark), std::span(zebra)));

    REQUIRE_FALSE(pisa::lex_lt(std::span(dog), std::span(dog)));
    REQUIRE_FALSE(pisa::lex_lt(std::span(dog), std::span(aardvark)));
    REQUIRE(pisa::lex_lt(std::span(dog), std::span(zebra)));

    REQUIRE_FALSE(pisa::lex_lt(std::span(zebra), std::span(zebra)));
    REQUIRE_FALSE(pisa::lex_lt(std::span(zebra), std::span(aardvark)));
    REQUIRE_FALSE(pisa::lex_lt(std::span(zebra), std::span(dog)));

    REQUIRE(pisa::lex_lt(std::span(empty), std::span(aardvark)));
    REQUIRE(pisa::lex_lt(std::span(empty), std::span(dog)));
    REQUIRE(pisa::lex_lt(std::span(empty), std::span(zebra)));
    REQUIRE_FALSE(pisa::lex_lt(std::span(aardvark), std::span(empty)));
    REQUIRE_FALSE(pisa::lex_lt(std::span(dog), std::span(empty)));
    REQUIRE_FALSE(pisa::lex_lt(std::span(zebra), std::span(empty)));
    REQUIRE_FALSE(pisa::lex_lt(std::span(empty), std::span(empty)));
}

TEST_CASE("lex_lt sort", "[span]") {
    std::vector<std::span<char const>> animals{
        "aardvark", "dog", "zebra", "pelican", "goose", "geese", "cat"
    };
    std::sort(animals.begin(), animals.end(), pisa::lex_lt<char const>);
    REQUIRE(animals[0] == std::span<char const>("aardvark"));
    REQUIRE(animals[1] == std::span<char const>("cat"));
    REQUIRE(animals[2] == std::span<char const>("dog"));
    REQUIRE(animals[3] == std::span<char const>("geese"));
    REQUIRE(animals[4] == std::span<char const>("goose"));
    REQUIRE(animals[5] == std::span<char const>("pelican"));
    REQUIRE(animals[6] == std::span<char const>("zebra"));
}
