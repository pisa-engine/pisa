#define CATCH_CONFIG_MAIN
#include "catch2/catch.hpp"

#include <tbb/task_scheduler_init.h>

#include "cursor/intersection.hpp"
#include "cursor/scored_cursor.hpp"
#include "cursor/span_cursor.hpp"
#include "in_memory_index.hpp"
#include "pisa_config.hpp"
#include "query/algorithm/ranked_and_query.hpp"
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

TEST_CASE("Execute on test index", "[cursor_intersection][integration]")
{
    tbb::task_scheduler_init init(1);
    for (auto &&scorer_name : {"bm25"}) {
        auto data = IndexData<single_index>::get(scorer_name);
        ranked_and_query and_q(10);

        with_scorer(scorer_name, data->wdata, [&](auto scorer) {
            for (auto const &q : data->queries) {
                auto and_cursors = make_scored_cursors(data->index, scorer, q);
                and_q(gsl::make_span(and_cursors), data->index.num_docs());

                auto cursors = make_scored_cursors(data->index, scorer, q);
                CursorIntersection i(std::move(cursors),
                                     data->index.num_docs(),
                                     float(0),
                                     [](float acc, auto &cursor, auto idx) {
                                         return acc + cursor.scorer(cursor.docid(), cursor.freq());
                                     });
                std::vector<std::pair<float, std::uint64_t>> results;
                while (i.docid() < data->index.num_docs()) {
                    results.push_back({i.payload(), i.docid()});
                    i.next();
                }
                std::sort(results.begin(), results.end(), std::greater{});

                for (size_t i = 0; i < and_q.topk().size(); ++i) {
                    CAPTURE(i);
                    CAPTURE(and_q.topk()[i].first);
                    CAPTURE(results[i].first);
                    CAPTURE(and_q.topk()[i].second);
                    CAPTURE(results[i].second);
                    REQUIRE(and_q.topk()[i].first == Approx(results[i].first).epsilon(0.1));
                }
            }
        });
    }
}
