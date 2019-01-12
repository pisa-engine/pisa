#define CATCH_CONFIG_MAIN
#include "catch2/catch.hpp"

#include <string>

#include "parsing/html.hpp"

using namespace ds2i::parsing::html;

TEST_CASE("Parse WARC version", "[warc][unit]")
{
    auto [input, expected] = GENERATE(table<std::string, std::string>(
        {{"text", "text"}, {"<a>text</a>", "text"}, {"<a>text</a>text", "text text"}}));
    GIVEN("Input: " << input) { CHECK(cleantext(input) == expected); }
}
