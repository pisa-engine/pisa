#define CATCH_CONFIG_MAIN
#include "catch2/catch.hpp"

#include "pisa/util/json_stats.hpp"

TEST_CASE("json_stats", "[unit]") {
    REQUIRE(
        pisa::json_stats()
            .add("bool", false)
            .add("int", 1)
            .add("long", std::numeric_limits<long>::max())
            .add("float", 1.0)
            .add("double", 10.1)
            .add("const char*", "hello")
            .add("string", std::string("hello"))
            .str()
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
