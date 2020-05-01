#define CATCH_CONFIG_MAIN

#include <sstream>

#include <catch2/catch.hpp>

#include "io.hpp"
#include "query/term_resolver.hpp"
#include "temporary_directory.hpp"

using pisa::QueryContainer;
using pisa::StandardTermResolver;

TEST_CASE("Filter queries")
{
    std::uint32_t id = 0;
    auto term_resolver = [&id](auto&& term) mutable {
        return std::optional<pisa::ResolvedTerm>{pisa::ResolvedTerm{id++, term}};
    };
    Temporary_Directory tmp;
    auto input = (tmp.path() / "input.txt");
    {
        std::ofstream os(input.c_str());
        os << "a b c d\n";
        os << "e\n";
        os << "f g h i j\n";
        os << "k l m\n";
        os << "n o\n";
    }

    SECTION("Between 2 and 4")
    {
        std::ostringstream os;
        pisa::filter_queries(
            std::make_optional(input.string()), std::make_optional(term_resolver), 2, 4, os);
        std::vector<QueryContainer> queries;
        std::istringstream is(os.str());
        pisa::io::for_each_line(
            is, [&queries](auto&& line) { queries.push_back(QueryContainer::from_json(line)); });
        REQUIRE(queries.size() == 3);
        REQUIRE(*queries[0].terms() == std::vector<std::string>{"a", "b", "c", "d"});
        REQUIRE(*queries[0].term_ids() == std::vector<std::uint32_t>{0, 1, 2, 3});
        REQUIRE(*queries[1].terms() == std::vector<std::string>{"k", "l", "m"});
        REQUIRE(*queries[1].term_ids() == std::vector<std::uint32_t>{10, 11, 12});
        REQUIRE(*queries[2].terms() == std::vector<std::string>{"n", "o"});
        REQUIRE(*queries[2].term_ids() == std::vector<std::uint32_t>{13, 14});

        SECTION("Don't fail if no resolver but IDs already resolved")
        {
            auto json_input = (tmp.path() / "input.json");
            {
                std::ofstream json_out(json_input.c_str());
                for (auto&& query: queries) {
                    json_out << query.to_json() << '\n';
                }
            }
            std::ostringstream output;
            pisa::filter_queries(std::make_optional(json_input.string()), std::nullopt, 2, 4, output);
            REQUIRE(output.str() == os.str());
        }
    }

    SECTION("Fail without IDs and resolver")
    {
        REQUIRE_THROWS_AS(
            pisa::filter_queries(std::make_optional(input.string()), std::nullopt, 2, 4, std::cerr),
            pisa::MissingResolverError);
    }
}
