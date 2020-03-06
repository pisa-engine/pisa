#define CATCH_CONFIG_MAIN
#include "catch2/catch.hpp"

#include <string>

#include "parsing/html.hpp"

using namespace pisa::parsing::html;

TEST_CASE("Parse HTML", "[html][unit]")
{
    auto [input, expected] =
        GENERATE(table<std::string, std::string>({{"text", "text"},
                                                  {"<a>text</a>", "text"},
                                                  {"<a>text</a>text", "text text"},
                                                  {"<a><!-- comment --></a>", ""},
                                                  {"<a><!-- comment --></a>", ""}}));
    GIVEN("Input: " << input) { CHECK(cleantext(input) == expected); }
}
