#include <catch2/catch_test_macros.hpp>

#include "../tools/query_safety.hpp"

TEST_CASE("query benchmark summary reports zero-threshold runs as safe") {
    REQUIRE(pisa::queries::summary_is_safe(false, false));
    REQUIRE(pisa::queries::summary_is_safe(true, true));
    REQUIRE_FALSE(pisa::queries::summary_is_safe(false, true));
}
