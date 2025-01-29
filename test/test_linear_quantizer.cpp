#define CATCH_CONFIG_MAIN
#include "catch2/catch.hpp"

#include <limits>

#include "linear_quantizer.hpp"

TEST_CASE("LinearQuantizer", "[scoring][unit]") {
    SECTION("construct") {
        WHEN("number of bits is 0, 1 or 33") {
            std::uint8_t bits = GENERATE(0, 1, 33);
            THEN("constructor fails") {
                REQUIRE_THROWS(pisa::LinearQuantizer(10.0, bits));
            }
        }
    }
    SECTION("max is 0") {
        std::uint8_t bits = 8;
        float max = 0.0;
        REQUIRE_THROWS(pisa::LinearQuantizer(max, bits));
    }
    SECTION("scores") {
        std::uint8_t bits = GENERATE(3, 8, 12, 16, 19, 32);
        float max = GENERATE(1.0, 100.0, std::numeric_limits<float>::max());
        CAPTURE((int)bits);
        CAPTURE(max);
        pisa::LinearQuantizer quantizer(max, bits);
        REQUIRE(quantizer(0) == 1);
        REQUIRE(quantizer(max) == (1ULL << bits) - 1);
    }
}
