#define CATCH_CONFIG_MAIN

#include <catch2/catch.hpp>
#include "query/queries.hpp"
#include "temporary_directory.hpp"

using namespace pisa;

TEST_CASE("Parse query term ids without query id") {
    auto raw_query = "1\t2\t3\t4";
    std::optional<std::string> id = std::nullopt;
    auto q = parse_query_ids(raw_query);
    REQUIRE(q.id.has_value() == false);
    REQUIRE(q.terms == std::vector<std::uint32_t>{1, 2, 3, 4});
}

TEST_CASE("Parse query term ids with query id") {
    auto raw_query = "1: 1\t2\t3\t4";
    std::optional<std::string> id = std::nullopt;
    auto q = parse_query_ids(raw_query);
    REQUIRE(q.id == "1");
    REQUIRE(q.terms == std::vector<std::uint32_t>{1, 2, 3, 4});
}