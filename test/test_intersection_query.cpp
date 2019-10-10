#define CATCH_CONFIG_MAIN
#include "catch2/catch.hpp"

#include "cursor/scored_cursor.hpp"
#include "cursor/span_cursor.hpp"
#include "cursor/union.hpp"
#include "in_memory_index.hpp"
#include "query/algorithm/inter_query.hpp"
#include "query/algorithm/ranked_or_query.hpp"
#include "query/queries.hpp"
#include "scorer/bm25.hpp"

using namespace pisa;

TEST_CASE("Extract IDs from a intersection bitset", "[intersection_query][unit]")
{
    REQUIRE(extract_ids({0b000}, 3) == std::vector<std::uint32_t>{});
    REQUIRE(extract_ids({0b001}, 3) == std::vector<std::uint32_t>{0});
    REQUIRE(extract_ids({0b010}, 3) == std::vector<std::uint32_t>{1});
    REQUIRE(extract_ids({0b100}, 3) == std::vector<std::uint32_t>{2});
    REQUIRE(extract_ids({0b011}, 3) == std::vector<std::uint32_t>{0, 1});
    REQUIRE(extract_ids({0b101}, 3) == std::vector<std::uint32_t>{0, 2});
    REQUIRE(extract_ids({0b110}, 3) == std::vector<std::uint32_t>{1, 2});
    REQUIRE(extract_ids({0b111}, 3) == std::vector<std::uint32_t>{0, 1, 2});
}

TEST_CASE("Safe intersections", "[cursor_union][unit]")
{
    auto intersections =
        GENERATE(std::vector<std::bitset<64>>{{0b001}, {0b010}, {0b100}},
                 std::vector<std::bitset<64>>{{0b011}, {0b100}, {0b001}, {0b110}},
                 std::vector<std::bitset<64>>{
                     {0b001}, {0b010}, {0b100}, {0b011}, {0b110}, {0b101}, {0b111}});

    InMemoryIndex index{
        {{0, 2, 4, 6}, {0, 2, 10}, {2, 4, 10}}, {{1, 1, 1, 1}, {1, 1, 1}, {1, 1, 1}}, 100};
    InMemoryWand wand(index);
    bm25<InMemoryWand> scorer(wand);
    auto results = intersection_query(index, Query{{}, {0, 1, 2}, {}}, intersections, scorer, 10);
    std::sort(results.begin(), results.end(), [](auto const &lhs, auto const &rhs) {
        return lhs.first > rhs.first;
    });

    ranked_or_query q(10);
    auto cursors = make_scored_cursors(index, scorer, Query{std::nullopt, {0, 1, 2}, {}});
    q(gsl::make_span(cursors), index.num_docs());
    std::vector<std::pair<float, std::uint64_t>> expected(q.topk().begin(), q.topk().end());

    REQUIRE(results == expected);
}

TEST_CASE("k = 2", "[cursor_union][unit]")
{
    // Here, k = 2 because when using only bigram intersections, these are the only results
    // that will be returned; in other words, it's unsafe to call it with only bigrams
    // in this case.
    int k = 2;

    // The additional intersection of all three terms does not change anything.
    auto intersections = GENERATE(std::vector<std::bitset<64>>{{0b011}, {0b110}, {0b101}},
                                  std::vector<std::bitset<64>>{{0b011}, {0b110}, {0b101}, {0b111}});
    InMemoryIndex index{
        {{0, 2, 4, 6}, {1, 2, 3}, {2, 4, 10}}, {{1, 1, 1, 1}, {1, 1, 1}, {1, 1, 1}}, 100};
    InMemoryWand wand(index);
    bm25<InMemoryWand> scorer(wand);
    auto results = intersection_query(index, Query{{}, {0, 1, 2}, {}}, intersections, scorer, k);
    std::sort(results.begin(), results.end(), [](auto const &lhs, auto const &rhs) {
        return lhs.first > rhs.first;
    });

    ranked_or_query q(k);
    auto cursors = make_scored_cursors(index, scorer, Query{std::nullopt, {0, 1, 2}, {}});
    q(gsl::make_span(cursors), index.num_docs());
    std::vector<std::pair<float, std::uint64_t>> expected(q.topk().begin(), q.topk().end());

    REQUIRE(results == expected);
}
