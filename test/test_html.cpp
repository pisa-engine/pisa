#include <exception>
#include <string>

#include "catch2/catch_all.hpp"

#include "parsing/html.hpp"

using namespace pisa::parsing::html;

TEST_CASE("Parse HTML", "[html][unit]") {
    auto [input, expected] = GENERATE(
        table<std::string, std::string>(
            {{"text", "text"},
             {"<a>text</a>", "text"},
             {"<a>text</a>text", "text text"},
             {"<a><!-- comment --></a>", ""},
             {"<a><!-- comment --></a>", ""}}
        )
    );
    GIVEN("Input: " << input) {
        CHECK(cleantext(input) == expected);
    }
}
