#define CATCH_CONFIG_MAIN

#include <sstream>

#include <catch2/catch.hpp>

#include "query/query_parser.hpp"

using pisa::QueryContainer;
using pisa::QueryParser;

TEST_CASE("Parse with lower-case processor and stop word")
{
    std::uint32_t init_id = 0;
    auto term_proc = [id = init_id](auto&& term) mutable {
        std::transform(
            term.begin(), term.end(), term.begin(), [](unsigned char c) { return std::tolower(c); });
        if (term == "house") {
            return std::optional<pisa::ResolvedTerm>{};
        }
        return std::optional<pisa::ResolvedTerm>{pisa::ResolvedTerm{id++, term}};
    };
    QueryParser parser(term_proc);
    auto terms = parser("Brooklyn tea house");
    REQUIRE(terms.size() == 2);
    REQUIRE(terms[0].term == "brooklyn");
    REQUIRE(terms[1].term == "tea");
}
