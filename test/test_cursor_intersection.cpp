#define CATCH_CONFIG_MAIN
#include "catch2/catch.hpp"

#include "cursor/intersection.hpp"
#include "cursor/scored_cursor.hpp"
#include "cursor/span_cursor.hpp"
#include "in_memory_index.hpp"
#include "query/algorithm/ranked_and_query.hpp"
#include "scorer/bm25.hpp"

using namespace pisa;

TEST_CASE("Single list", "[cursor_intersection][unit]")
{
    std::vector<std::vector<std::uint32_t>> documents{{0, 2, 4, 6}, {1, 2, 3, 4}, {2, 4, 10}};
    std::vector<std::vector<std::uint32_t>> frequencies{{1, 1, 1, 1}, {1, 1, 1, 1}, {1, 1, 1}};

    std::uint32_t max_docid = 100;
    std::vector<SpanCursor> cursors{SpanCursor(documents[0], frequencies[0], max_docid)};
    CursorIntersection i(
        gsl::make_span(cursors),
        max_docid,
        std::uint32_t(0),
        [](std::uint32_t acc, auto &cursor, auto idx) { return acc + cursor.freq(); });
    REQUIRE(i.docid() == 0);
    REQUIRE(i.payload() == 1);
    i.next();
    REQUIRE(i.docid() == 2);
    REQUIRE(i.payload() == 1);
    i.next();
    REQUIRE(i.docid() == 4);
    REQUIRE(i.payload() == 1);
    i.next();
    REQUIRE(i.docid() == 6);
    REQUIRE(i.payload() == 1);
    i.next();
    REQUIRE(i.docid() == 100);
    REQUIRE(i.payload() == 0);
}

TEST_CASE("Single list -- vector", "[cursor_intersection][unit]")
{
    std::vector<std::vector<std::uint32_t>> documents{{0, 2, 4, 6}, {1, 2, 3, 4}, {2, 4, 10}};
    std::vector<std::vector<std::uint32_t>> frequencies{{1, 1, 1, 1}, {1, 1, 1, 1}, {1, 1, 1}};

    using payload_type = std::vector<std::int32_t>;
    std::uint32_t max_docid = 100;
    std::vector<SpanCursor> cursors{SpanCursor(documents[0], frequencies[0], max_docid)};
    CursorIntersection i(gsl::make_span(cursors),
                         max_docid,
                         payload_type{},
                         [](payload_type &acc, auto &cursor, auto idx) {
                             acc.push_back(cursor.freq());
                             return acc;
                         });
    REQUIRE(i.docid() == 0);
    REQUIRE(i.payload() == payload_type{1});
    i.next();
    REQUIRE(i.docid() == 2);
    REQUIRE(i.payload() == payload_type{1});
    i.next();
    REQUIRE(i.docid() == 4);
    REQUIRE(i.payload() == payload_type{1});
    i.next();
    REQUIRE(i.docid() == 6);
    REQUIRE(i.payload() == payload_type{1});
    i.next();
    REQUIRE(i.docid() == 100);
    REQUIRE(i.payload() == payload_type{});
}

TEST_CASE("add frequencies", "[cursor_intersection][unit]")
{
    std::vector<std::vector<std::uint32_t>> documents{{0, 2, 4, 6}, {1, 2, 3, 4}, {2, 4, 10}};
    std::vector<std::vector<std::uint32_t>> frequencies{{1, 1, 1, 1}, {1, 1, 1, 1}, {1, 1, 1}};

    std::uint32_t max_docid = 100;
    std::vector<SpanCursor> cursors{SpanCursor(documents[0], frequencies[0], max_docid),
                                    SpanCursor(documents[1], frequencies[1], max_docid),
                                    SpanCursor(documents[2], frequencies[2], max_docid)};
    CursorIntersection i(
        gsl::make_span(cursors),
        max_docid,
        std::uint32_t(0),
        [](std::uint32_t acc, auto &cursor, auto idx) { return acc + cursor.freq(); });
    REQUIRE(i.docid() == 2);
    REQUIRE(i.payload() == 3);
    i.next();
    REQUIRE(i.docid() == 4);
    REQUIRE(i.payload() == 3);
    i.next();
    REQUIRE(i.docid() == 100);
    REQUIRE(i.payload() == 0);
}

TEST_CASE("Ranked AND query", "[cursor_intersection][unit]")
{
    InMemoryIndex index{
        {{0, 2, 4, 6}, {1, 2, 3, 4}, {2, 4, 10}}, {{1, 1, 1, 1}, {1, 1, 1, 1}, {1, 1, 1}}, 100};
    InMemoryWand wand(index);

    bm25<InMemoryWand> scorer(wand);
    auto cursors = make_scored_cursors(index, scorer, Query{std::nullopt, {0, 1, 2}, {}});
    CursorIntersection i(
        gsl::make_span(cursors), index.num_docs(), float(0), [](float acc, auto &cursor, auto idx) {
            return acc + cursor.scorer(cursor.docid(), cursor.freq());
        });
    std::vector<std::pair<float, std::uint64_t>> results;
    while (i.docid() < index.num_docs()) {
        results.push_back({i.payload(), i.docid()});
        i.next();
    }
    std::sort(results.begin(), results.end());

    ranked_and_query q(10);
    cursors = make_scored_cursors(index, scorer, Query{std::nullopt, {0, 1, 2}, {}});
    q(gsl::make_span(cursors), index.num_docs());

    std::vector<std::pair<float, std::uint64_t>> expected(q.topk().begin(), q.topk().end());
    std::sort(expected.begin(), expected.end());
    REQUIRE(results == expected);
}
