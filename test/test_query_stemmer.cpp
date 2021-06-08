#define CATCH_CONFIG_MAIN
#include "catch2/catch.hpp"
#include "parser.hpp"
#include "query/query_stemmer.hpp"
#include <string>

using namespace pisa;

TEST_CASE("Stem query", "[stemming][unit]")
{
    auto [input, expected] =
        GENERATE(table<std::string, std::string>({{"1:playing cards", "1:play card"},
                                                  {"playing cards", "play card"},
                                                  {"play card", "play card"},
                                                  {"1:this:that", "1:this that"}}));
    QueryStemmer query_stemmer("porter2");
    GIVEN("Input: " << input) { CHECK(query_stemmer(input) == expected); }
}
