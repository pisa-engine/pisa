#define CATCH_CONFIG_MAIN
#include "catch2/catch.hpp"

#include <tbb/task_scheduler_init.h>

#include "cursor/intersection.hpp"
#include "cursor/scored_cursor.hpp"
#include "cursor/span_cursor.hpp"
#include "cursor/union.hpp"
#include "in_memory_index.hpp"
#include "int_iter.hpp"
#include "pisa_config.hpp"
#include "query/algorithm/ranked_or_query.hpp"
#include "scorer/bm25.hpp"

using namespace pisa;

template <typename Index>
struct IndexData {
    static std::unordered_map<std::string, std::unique_ptr<IndexData>> data;

    explicit IndexData(std::string const &scorer_name)
        : collection(PISA_SOURCE_DIR "/test/test_data/test_collection"),
          document_sizes(PISA_SOURCE_DIR "/test/test_data/test_collection.sizes"),
          wdata(document_sizes.begin()->begin(),
                collection.num_docs(),
                collection,
                scorer_name,
                BlockSize(FixedBlock()))

    {
        typename Index::builder builder(collection.num_docs(), params);
        for (auto const &plist : collection) {
            uint64_t freqs_sum =
                std::accumulate(plist.frequencies.begin(), plist.frequencies.end(), uint64_t(0));
            builder.add_posting_list(plist.documents.size(),
                                     plist.documents.begin(),
                                     plist.frequencies.begin(),
                                     freqs_sum);
        }
        builder.build(index);

        term_id_vec q;
        std::ifstream qfile(PISA_SOURCE_DIR "/test/test_data/queries");
        auto push_query = [&](std::string const &query_line) {
            queries.push_back(parse_query_ids(query_line));
        };
        io::for_each_line(qfile, push_query);

        std::string t;
    }

    [[nodiscard]] static auto get(std::string const &s_name)
    {
        if (IndexData::data.find(s_name) == IndexData::data.end()) {
            IndexData::data[s_name] = std::make_unique<IndexData<Index>>(s_name);
        }
        return IndexData::data[s_name].get();
    }

    global_parameters params;
    BinaryFreqCollection collection;
    BinaryCollection document_sizes;
    Index index;
    std::vector<Query> queries;
    wand_data<wand_data_raw> wdata;
};

template <typename Index>
std::unordered_map<std::string, unique_ptr<IndexData<Index>>> IndexData<Index>::data = {};

TEST_CASE("Single list", "[cursor_union][unit]")
{
    std::vector<std::vector<std::uint32_t>> documents{{0, 2, 4, 6}, {1, 2, 3}, {2, 4, 10}};
    std::vector<std::vector<std::uint32_t>> frequencies{{1, 1, 1, 1}, {1, 1, 1}, {1, 1, 1}};

    std::uint32_t max_docid = 100;
    std::vector<SpanCursor> cursors{SpanCursor(documents[0], frequencies[0], max_docid)};
    CursorUnion u(std::move(cursors),
                  max_docid,
                  std::uint32_t(0),
                  [](std::uint32_t acc, auto &cursor, auto idx) { return acc + cursor.freq(); });
    REQUIRE(u.docid() == 0);
    REQUIRE(u.payload() == 1);
    u.next();
    REQUIRE(u.docid() == 2);
    REQUIRE(u.payload() == 1);
    u.next();
    REQUIRE(u.docid() == 4);
    REQUIRE(u.payload() == 1);
    u.next();
    REQUIRE(u.docid() == 6);
    REQUIRE(u.payload() == 1);
    u.next();
    REQUIRE(u.docid() == 100);
    REQUIRE(u.payload() == 0);
}

TEST_CASE("Single list -- accumulate to vector", "[cursor_union][unit]")
{
    std::vector<std::vector<std::uint32_t>> documents{{0, 2, 4, 6}, {1, 2, 3}, {2, 4, 10}};
    std::vector<std::vector<std::uint32_t>> frequencies{{1, 1, 1, 1}, {1, 1, 1}, {1, 1, 1}};

    using payload_type = std::vector<std::int32_t>;
    std::uint32_t max_docid = 100;
    std::vector<SpanCursor> cursors{SpanCursor(documents[0], frequencies[0], max_docid)};
    CursorUnion u(std::move(cursors),
                  max_docid,
                  payload_type{},
                  [](payload_type &acc, auto &cursor, auto idx) {
                      acc.push_back(cursor.freq());
                      return acc;
                  });
    REQUIRE(u.docid() == 0);
    REQUIRE(u.payload() == payload_type{1});
    u.next();
    REQUIRE(u.docid() == 2);
    REQUIRE(u.payload() == payload_type{1});
    u.next();
    REQUIRE(u.docid() == 4);
    REQUIRE(u.payload() == payload_type{1});
    u.next();
    REQUIRE(u.docid() == 6);
    REQUIRE(u.payload() == payload_type{1});
    u.next();
    REQUIRE(u.docid() == 100);
    REQUIRE(u.payload() == payload_type{});
}

TEST_CASE("Union of intersections of single lists", "[cursor_union][integration]")
{
    std::vector<std::vector<std::uint32_t>> documents{{0, 2, 4, 6}, {1, 2, 3}, {2, 4, 10}};
    std::vector<std::vector<std::uint32_t>> frequencies{{1, 1, 1, 1}, {1, 1, 1}, {1, 1, 1}};

    auto freq = [](auto acc, auto &cursor, auto idx) { return acc + cursor.freq(); };
    auto payload = [](auto acc, auto &cursor, auto idx) { return acc + cursor.payload(); };
    std::uint32_t max_docid = 100;
    std::vector<SpanCursor> cursors{SpanCursor(documents[0], frequencies[0], max_docid),
                                    SpanCursor(documents[1], frequencies[1], max_docid),
                                    SpanCursor(documents[2], frequencies[2], max_docid)};
    std::vector<decltype(
        CursorIntersection(gsl::span(&cursors[0], 1), max_docid, std::uint32_t(0), freq))>
        intersections{
            CursorIntersection(gsl::span(&cursors[0], 1), max_docid, std::uint32_t(0), freq),
            CursorIntersection(gsl::span(&cursors[1], 1), max_docid, std::uint32_t(0), freq),
            CursorIntersection(gsl::span(&cursors[2], 1), max_docid, std::uint32_t(0), freq)};
    CursorUnion u(std::move(intersections), max_docid, std::uint32_t(0), payload);
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

TEST_CASE("Union of intersections of single lists -- vector", "[cursor_union][integration]")
{
    std::vector<std::vector<std::uint32_t>> documents{{0, 2, 4, 6}, {1, 2, 3}, {2, 4, 10}};
    std::vector<std::vector<std::uint32_t>> frequencies{{1, 1, 1, 1}, {1, 1, 1}, {1, 1, 1}};

    using payload_type = std::vector<std::pair<std::size_t, std::int32_t>>;
    auto freq = [](payload_type &acc, auto &cursor, auto idx) {
        acc.emplace_back(idx, cursor.freq());
        return acc;
    };
    auto payload = [](payload_type &acc, auto &cursor, auto idx) {
        auto &payload = cursor.payload();
        // Replace the idx with the idx of the intersection within union.
        std::transform(payload.begin(),
                       payload.end(),
                       std::back_inserter(acc),
                       [idx](auto const &single_payload) {
                           return std::make_pair(idx, single_payload.second);
                       });
        return acc;
    };
    std::uint32_t max_docid = 100;
    std::vector<SpanCursor> cursors{SpanCursor(documents[0], frequencies[0], max_docid),
                                    SpanCursor(documents[1], frequencies[1], max_docid),
                                    SpanCursor(documents[2], frequencies[2], max_docid)};
    std::vector<decltype(
        CursorIntersection(gsl::span(&cursors[0], 1), max_docid, payload_type{}, freq))>
        intersections{
            CursorIntersection(gsl::span(&cursors[0], 1), max_docid, payload_type{}, freq),
            CursorIntersection(gsl::span(&cursors[1], 1), max_docid, payload_type{}, freq),
            CursorIntersection(gsl::span(&cursors[2], 1), max_docid, payload_type{}, freq)};
    CursorUnion u(std::move(intersections), max_docid, payload_type{}, payload);
    REQUIRE(u.docid() == 0);
    REQUIRE(u.payload() == payload_type{{0, 1}});
    u.next();
    REQUIRE(u.docid() == 1);
    REQUIRE(u.payload() == payload_type{{1, 1}});
    u.next();
    REQUIRE(u.docid() == 2);
    REQUIRE(u.payload() == payload_type{{0, 1}, {1, 1}, {2, 1}});
    u.next();
    REQUIRE(u.docid() == 3);
    REQUIRE(u.payload() == payload_type{{1, 1}});
    u.next();
    REQUIRE(u.docid() == 4);
    REQUIRE(u.payload() == payload_type{{0, 1}, {2, 1}});
    u.next();
    REQUIRE(u.docid() == 6);
    REQUIRE(u.payload() == payload_type{{0, 1}});
    u.next();
    REQUIRE(u.docid() == 10);
    REQUIRE(u.payload() == payload_type{{2, 1}});
    u.next();
    REQUIRE(u.docid() == 100);
    REQUIRE(u.payload() == payload_type{});
}

TEST_CASE("add frequencies", "[cursor_union][unit]")
{
    std::vector<std::vector<std::uint32_t>> documents{{0, 2, 4, 6}, {1, 2, 3}, {2, 4, 10}};
    std::vector<std::vector<std::uint32_t>> frequencies{{1, 1, 1, 1}, {1, 1, 1}, {1, 1, 1}};

    std::uint32_t max_docid = 100;
    std::vector<SpanCursor> cursors{SpanCursor(documents[0], frequencies[0], max_docid),
                                    SpanCursor(documents[1], frequencies[1], max_docid),
                                    SpanCursor(documents[2], frequencies[2], max_docid)};
    CursorUnion u(std::move(cursors),
                  max_docid,
                  std::uint32_t(0),
                  [](std::uint32_t acc, auto &cursor, auto idx) { return acc + cursor.freq(); });
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
    CursorUnion u(
        std::move(cursors), index.num_docs(), float(0), [](float acc, auto &cursor, auto idx) {
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

TEST_CASE("Execute on test index", "[cursor_union][integration]")
{
    tbb::task_scheduler_init init(1);
    for (auto &&scorer_name : {"bm25"}) {
        auto data = IndexData<single_index>::get(scorer_name);
        ranked_or_query or_q(10);

        with_scorer(scorer_name, data->wdata, [&](auto scorer) {
            for (auto const &q : data->queries) {
                auto or_cursors = make_scored_cursors(data->index, scorer, q);
                or_q(gsl::make_span(or_cursors), data->index.num_docs());

                auto cursors = make_scored_cursors(data->index, scorer, q);
                CursorUnion u(std::move(cursors),
                              data->index.num_docs(),
                              float(0),
                              [](float acc, auto &cursor, auto idx) {
                                  return acc + cursor.scorer(cursor.docid(), cursor.freq());
                              });
                std::vector<std::pair<float, std::uint64_t>> results;
                while (u.docid() < data->index.num_docs()) {
                    results.push_back({u.payload(), u.docid()});
                    u.next();
                }
                std::sort(results.begin(), results.end(), std::greater{});

                for (size_t i = 0; i < or_q.topk().size(); ++i) {
                    CAPTURE(i);
                    CAPTURE(or_q.topk()[i].first);
                    CAPTURE(results[i].first);
                    CAPTURE(or_q.topk()[i].second);
                    CAPTURE(results[i].second);
                    REQUIRE(or_q.topk()[i].first == Approx(results[i].first).epsilon(0.1));
                }
            }
        });
    }
}
