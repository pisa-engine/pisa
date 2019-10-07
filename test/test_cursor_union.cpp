#define CATCH_CONFIG_MAIN
#include "catch2/catch.hpp"

#include "cursor/scored_cursor.hpp"
#include "cursor/span_cursor.hpp"
#include "cursor/union.hpp"
#include "in_memory_index.hpp"
#include "query/algorithm/ranked_or_query.hpp"
#include "scorer/bm25.hpp"

using namespace pisa;

TEST_CASE("add frequencies", "[cursor_union][unit]")
{
    std::vector<std::vector<std::uint32_t>> documents{{0, 2, 4, 6}, {1, 2, 3}, {2, 4, 10}};
    std::vector<std::vector<std::uint32_t>> frequencies{{1, 1, 1, 1}, {1, 1, 1}, {1, 1, 1}};

    std::uint32_t max_docid = 100;
    std::vector<SpanCursor> cursors{SpanCursor(documents[0], frequencies[0], max_docid),
                                    SpanCursor(documents[1], frequencies[1], max_docid),
                                    SpanCursor(documents[2], frequencies[2], max_docid)};
    CursorUnion u(gsl::make_span(cursors),
                  max_docid,
                  std::uint32_t(0),
                  [](std::uint32_t acc, auto &cursor) { return acc + cursor.freq(); });
    REQUIRE(u.docid() == 0);
    REQUIRE(u.payload() == 1);
    u.next();
    REQUIRE(u.docid() == 1);
    REQUIRE(u.payload() == 1);
    u.next();
    REQUIRE(u.docid() == 2);
    REQUIRE(u.payload() == 3);
    u.next();
    REQUIRE(u.docid() == 3);
    REQUIRE(u.payload() == 1);
    u.next();
    REQUIRE(u.docid() == 4);
    REQUIRE(u.payload() == 2);
    u.next();
    REQUIRE(u.docid() == 6);
    REQUIRE(u.payload() == 1);
    u.next();
    REQUIRE(u.docid() == 10);
    REQUIRE(u.payload() == 1);
    u.next();
    REQUIRE(u.docid() == 100);
    REQUIRE(u.payload() == 0);
}

TEST_CASE("Ranked OR query", "[cursor_union][unit]")
{
    InMemoryIndex index{
        {{0, 2, 4, 6}, {1, 2, 3}, {2, 4, 10}}, {{1, 1, 1, 1}, {1, 1, 1}, {1, 1, 1}}, 100};
    InMemoryWand wand(index);

    bm25<InMemoryWand> scorer(wand);
    auto cursors = make_scored_cursors(index, scorer, Query{std::nullopt, {0, 1, 2}, {}});
    CursorUnion u(gsl::make_span(cursors), index.num_docs(), float(0), [](float acc, auto &cursor) {
        return acc + cursor.scorer(cursor.docid(), cursor.freq());
    });
    std::vector<std::pair<float, std::uint64_t>> results;
    while (u.docid() < index.num_docs()) {
        results.push_back({u.payload(), u.docid()});
        u.next();
    }
    std::sort(results.begin(), results.end());
    ranked_or_query q(10);
    cursors = make_scored_cursors(index, scorer, Query{std::nullopt, {0, 1, 2}, {}});
    q(gsl::make_span(cursors), index.num_docs());

    std::vector<std::pair<float, std::uint64_t>> expected(q.topk().begin(), q.topk().end());
    std::sort(expected.begin(), expected.end());
    REQUIRE(results == expected);
}
