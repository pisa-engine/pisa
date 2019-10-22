#define CATCH_CONFIG_MAIN
#include "catch2/catch.hpp"

#include "v1/index.hpp"

using pisa::v1::RawReader;

TEST_CASE("RawReader", "[v1][unit]")
{
    std::vector<std::uint64_t> const mem{0, 1, 2, 3, 4};
    RawReader<uint8_t> reader;
    auto cursor = reader.read(gsl::as_bytes(gsl::make_span(mem)));
    REQUIRE(cursor.next() == tl::make_optional(mem[0]));
    REQUIRE(cursor.next() == tl::make_optional(mem[1]));
    REQUIRE(cursor.next() == tl::make_optional(mem[2]));
    REQUIRE(cursor.next() == tl::make_optional(mem[3]));
    REQUIRE(cursor.next() == tl::make_optional(mem[4]));
    REQUIRE(cursor.next() == tl::nullopt);
}
