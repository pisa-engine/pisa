#define CATCH_CONFIG_MAIN
#include "catch2/catch.hpp"

#include <limits>

#include "linear_quantizer.hpp"

TEST_CASE("LinearQuantizer", "[scoring][unit]") {
    SECTION("construct") {
        WHEN("number of bits is 0 or 33") {
            std::uint8_t bits = GENERATE(0, 33);
            THEN("constructor fails") {
                REQUIRE_THROWS(pisa::LinearQuantizer(10.0, bits));
            }
        }
    }
    SECTION("scores") {
        std::uint8_t bits = GENERATE(3, 8, 12, 16, 19, 32);
        float max = GENERATE(1.0, 100.0, std::numeric_limits<float>::max());
        pisa::LinearQuantizer quantizer(max, bits);
        REQUIRE(quantizer(0) == 0);
        REQUIRE(quantizer(max) == (1 << bits) - 1);
    }
}
