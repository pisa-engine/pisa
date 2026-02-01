#define CATCH_CONFIG_MAIN
#include "catch2/catch.hpp"

#include "pisa/util/stats_builder.hpp"

TEST_CASE("stats_builder", "[unit]") {
    REQUIRE(
        pisa::stats_builder()
            .add("bool", false)
            .add("int", 1)
            .add("long", std::numeric_limits<long>::max())
            .add("float", 1.0)
            .add("double", 10.1)
            .add("const char*", "hello")
            .add("string", std::string("hello"))
            .build()
        == R"({
  "bool": false,
  "const char*": "hello",
  "double": 10.1,
  "float": 1.0,
  "int": 1,
  "long": 9223372036854775807,
  "string": "hello"
})"
    );
}
