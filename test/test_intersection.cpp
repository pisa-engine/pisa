#define CATCH_CONFIG_MAIN
#include "catch2/catch.hpp"

#include <fmt/format.h>
#include <gsl/span>

#include "cursor/scored_cursor.hpp"
#include "in_memory_index.hpp"
#include "intersection.hpp"

using namespace pisa;
using namespace pisa::intersection;

TEST_CASE("filter query", "[intersection][unit]")
{
    GIVEN("Four-term query")
    {
        Query query{
            "Q1",  // query ID
            {6, 1, 5},  // terms
            {0.1, 0.4, 1.0}  // weights
        };
        auto [mask, expected] = GENERATE(table<Mask, Query>({
            {0b001, Query{"Q1", {6}, {0.1}}},
            {0b010, Query{"Q1", {1}, {0.4}}},
            {0b100, Query{"Q1", {5}, {1.0}}},
            {0b011, Query{"Q1", {6, 1}, {0.1, 0.4}}},
            {0b101, Query{"Q1", {6, 5}, {0.1, 1.0}}},
            {0b110, Query{"Q1", {1, 5}, {0.4, 1.0}}},
            {0b111, Query{"Q1", {6, 1, 5}, {0.1, 0.4, 1.0}}},
        }));
        WHEN("Filtered with mask " << mask)
        {
            auto actual = filter(query, mask);
            CHECK(actual.id == expected.id);
            CHECK(actual.terms == expected.terms);
            CHECK(actual.term_weights == expected.term_weights);
        }
    }
}

TEST_CASE("Vector cursor", "[intersection][unit]")
{
    std::vector<std::uint32_t> documents{0, 3, 5, 6, 87, 111};
    std::vector<std::uint32_t> frequencies{1, 4, 6, 7, 88, 112};

    auto cursor = VectorCursor{gsl::make_span(documents), gsl::make_span(frequencies), 200, {200}};

    REQUIRE(cursor.size() == 6);

    REQUIRE(cursor.docid() == 0);
    REQUIRE(cursor.freq() == 1);

    cursor.next();
    REQUIRE(cursor.docid() == 3);
    REQUIRE(cursor.freq() == 4);

    cursor.next();
    REQUIRE(cursor.docid() == 5);
    REQUIRE(cursor.freq() == 6);

    cursor.next();
    REQUIRE(cursor.docid() == 6);
    REQUIRE(cursor.freq() == 7);

    cursor.next();
    REQUIRE(cursor.docid() == 87);
    REQUIRE(cursor.freq() == 88);

    cursor.next();
    REQUIRE(cursor.docid() == 111);
    REQUIRE(cursor.freq() == 112);

    cursor.next();
    REQUIRE(cursor.docid() == 200);

    cursor.next();
    REQUIRE(cursor.docid() == 200);

    // NEXTGEQ
    cursor = VectorCursor{gsl::make_span(documents), gsl::make_span(frequencies), 200, {200}};

    REQUIRE(cursor.docid() == 0);
    REQUIRE(cursor.freq() == 1);

    cursor.next_geq(4);
    REQUIRE(cursor.docid() == 5);
    REQUIRE(cursor.freq() == 6);

    cursor.next_geq(87);
    REQUIRE(cursor.docid() == 87);
    REQUIRE(cursor.freq() == 88);

    cursor.next_geq(178);
    REQUIRE(cursor.docid() == 200);
}

TEST_CASE("compute intersection", "[intersection][unit]")
{
    GIVEN("Four-term query, index, and wand data object")
    {
        InMemoryIndex index{{
                                {0},  // 0
                                {0, 1, 2},  // 1
                                {0},  // 2
                                {0},  // 3
                                {0},  // 4
                                {0, 1, 4},  // 5
                                {1, 4, 8},  // 6
                            },
                            {
                                {1},  // 0
                                {1, 1, 1},  // 1
                                {1},  // 2
                                {1},  // 3
                                {1},  // 4
                                {1, 1, 1},  // 5
                                {1, 1, 1},  // 6
                            },
                            10};
        InMemoryWand wand{{0.0, 1.0, 0.0, 0.0, 0.0, 5.0, 6.0}, 10};

        Query query{
            "Q1",  // query ID
            {6, 1, 5},  // terms
            {0.1, 0.4, 1.0}  // weights
        };
        auto [mask, len, max] = GENERATE(table<Mask, std::size_t, float>({
            {0b001, 3, 1.84583F},
            {0b010, 3, 1.84583F},
            {0b100, 3, 1.84583F},
            {0b011, 1, 3.69165F},
            {0b101, 2, 3.69165F},
            {0b110, 2, 3.69165F},
            {0b111, 1, 5.53748F},
        }));
        WHEN("Computed intersection with mask " << mask)
        {
            auto intersection = Intersection::compute(index, wand, query, mask);
            CHECK(intersection.length == len);
            CHECK(intersection.max_score == Approx(max));
        }
    }
}

TEST_CASE("for_all_subsets", "[intersection][unit]")
{
    GIVEN("A query and a mock function that accumulates arguments")
    {
        std::vector<Mask> masks;
        auto accumulate = [&](Query const&, Mask const& mask) { masks.push_back(mask); };
        Query query{
            "Q1",  // query ID
            {6, 1, 5},  // terms
            {0.1, 0.4, 1.0}  // weights
        };
        WHEN("Executed with limit 0")
        {
            for_all_subsets(query, 0, accumulate);
            THEN("No elements accumulated") { CHECK(masks.empty()); }
        }
        WHEN("Executed with limit 1")
        {
            for_all_subsets(query, 1, accumulate);
            THEN("Unigrams accumulated")
            {
                CHECK(masks == std::vector<Mask>{Mask(0b001), Mask(0b010), Mask(0b100)});
            }
        }
        WHEN("Executed with limit 2")
        {
            for_all_subsets(query, 2, accumulate);
            THEN("Unigrams and bigrams accumulated")
            {
                CHECK(
                    masks
                    == std::vector<Mask>{
                        Mask(0b001), Mask(0b010), Mask(0b011), Mask(0b100), Mask(0b101), Mask(0b110)});
            }
        }
        WHEN("Executed with limit 3")
        {
            for_all_subsets(query, 3, accumulate);
            THEN("All combinations accumulated")
            {
                CHECK(
                    masks
                    == std::vector<Mask>{Mask(0b001),
                                         Mask(0b010),
                                         Mask(0b011),
                                         Mask(0b100),
                                         Mask(0b101),
                                         Mask(0b110),
                                         Mask(0b111)});
            }
        }
    }
}
