#include <exception>

#include <catch2/catch_all.hpp>

#include "pisa/token_stream.hpp"

using namespace pisa;

TEST_CASE("EmptyTokenStream") {
    EmptyTokenStream empty;
    REQUIRE(empty.next() == std::nullopt);
}

TEST_CASE("SingleTokenStream") {
    SingleTokenStream single("token");
    REQUIRE(single.next() == "token");
    REQUIRE(single.next() == std::nullopt);
}
