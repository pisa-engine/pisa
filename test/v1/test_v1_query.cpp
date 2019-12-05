#define CATCH_CONFIG_MAIN
#include "catch2/catch.hpp"

#include <string>

#include <boost/iostreams/device/back_inserter.hpp>
#include <boost/iostreams/stream.hpp>
#include <gsl/span>
#include <rapidcheck.h>

#include "pisa_config.hpp"
#include "v1/query.hpp"

using pisa::v1::ListSelection;

TEST_CASE("List selections are overlapping", "[v1][unit]")
{
    REQUIRE(not ListSelection{.unigrams = {0, 1, 2}, .bigrams = {}}.overlapping());
    REQUIRE(not ListSelection{.unigrams = {0, 1, 2}, .bigrams = {{0, 1}, {2, 3}}}.overlapping());
    REQUIRE(ListSelection{.unigrams = {0, 1, 1, 2}, .bigrams = {}}.overlapping());
    REQUIRE(ListSelection{.unigrams = {0, 1, 2}, .bigrams = {{0, 3}}}.overlapping());
    REQUIRE(ListSelection{.unigrams = {}, .bigrams = {{0, 1}, {1, 3}}}.overlapping());
}
