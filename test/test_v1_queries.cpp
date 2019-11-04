#define CATCH_CONFIG_MAIN
#include "catch2/catch.hpp"

#include <string>

#include <boost/iostreams/device/back_inserter.hpp>
#include <boost/iostreams/stream.hpp>
#include <tbb/task_scheduler_init.h>

#include "accumulator/lazy_accumulator.hpp"
#include "cursor/block_max_scored_cursor.hpp"
#include "cursor/max_scored_cursor.hpp"
#include "cursor/scored_cursor.hpp"
#include "index_types.hpp"
#include "io.hpp"
#include "pisa_config.hpp"
#include "query/queries.hpp"
#include "v1/cursor/collect.hpp"
#include "v1/cursor_intersection.hpp"
#include "v1/cursor_union.hpp"
#include "v1/index.hpp"
#include "v1/posting_builder.hpp"
#include "v1/posting_format_header.hpp"
#include "v1/query.hpp"
#include "v1/scorer/bm25.hpp"
#include "v1/types.hpp"

namespace v1 = pisa::v1;
using namespace pisa;

template <typename v0_Index, typename v1_Index, typename ScoredIndex>
struct IndexData {

    static std::unique_ptr<IndexData> data;

    IndexData()
        : collection(PISA_SOURCE_DIR "/test/test_data/test_collection"),
          document_sizes(PISA_SOURCE_DIR "/test/test_data/test_collection.sizes"),
          v1_index(
              pisa::v1::binary_collection_index(PISA_SOURCE_DIR "/test/test_data/test_collection")),
          scored_index(pisa::v1::binary_collection_scored_index(PISA_SOURCE_DIR
                                                                "/test/test_data/test_collection")),
          wdata(document_sizes.begin()->begin(),
                collection.num_docs(),
                collection,
                BlockSize(FixedBlock()))

    {
        tbb::task_scheduler_init init;
        typename v0_Index::builder builder(collection.num_docs(), params);
        for (auto const &plist : collection) {
            uint64_t freqs_sum =
                std::accumulate(plist.freqs.begin(), plist.freqs.end(), uint64_t(0));
            builder.add_posting_list(
                plist.docs.size(), plist.docs.begin(), plist.freqs.begin(), freqs_sum);
        }
        builder.build(v0_index);

        term_id_vec q;
        std::ifstream qfile(PISA_SOURCE_DIR "/test/test_data/queries");
        auto push_query = [&](std::string const &query_line) {
            queries.push_back(parse_query_ids(query_line));
        };
        io::for_each_line(qfile, push_query);

        std::string t;
        std::ifstream tin(PISA_SOURCE_DIR "/test/test_data/top5_thresholds");
        while (std::getline(tin, t)) {
            thresholds.push_back(std::stof(t));
        }
    }

    [[nodiscard]] static auto get()
    {
        if (IndexData::data == nullptr) {
            IndexData::data = std::make_unique<IndexData<v0_Index, v1_Index, ScoredIndex>>();
        }
        return IndexData::data.get();
    }

    global_parameters params;
    binary_freq_collection collection;
    binary_collection document_sizes;
    v0_Index v0_index;
    v1_Index v1_index;
    ScoredIndex scored_index;
    std::vector<Query> queries;
    std::vector<float> thresholds;
    wand_data<wand_data_raw> wdata;
};

template <typename v0_Index, typename v1_Index, typename ScoredIndex>
std::unique_ptr<IndexData<v0_Index, v1_Index, ScoredIndex>>
    IndexData<v0_Index, v1_Index, ScoredIndex>::data = nullptr;

TEST_CASE("DAAT AND", "[v1][integration]")
{
    auto data = IndexData<single_index,
                          v1::Index<v1::RawCursor<v1::DocId>, v1::RawCursor<v1::Frequency>>,
                          v1::Index<v1::RawCursor<v1::DocId>, v1::RawCursor<float>>>::get();
    ranked_and_query and_q(10);
    int idx = 0;
    for (auto const &q : data->queries) {

        CAPTURE(q.terms);
        CAPTURE(idx++);

        and_q(make_scored_cursors(data->v0_index, data->wdata, q), data->v0_index.num_docs());
        auto expected = and_q.topk();
        std::sort(expected.begin(), expected.end(), std::greater{});

        auto on_the_fly = [&]() {
            auto que = daat_and(
                v1::Query{q.terms}, data->v1_index, topk_queue(10), make_bm25(data->v1_index));
            que.finalize();
            auto results = que.topk();
            std::sort(results.begin(), results.end(), std::greater{});
            return results;
        }();

        auto precomputed = [&]() {
            auto que =
                daat_and(v1::Query{q.terms}, data->scored_index, topk_queue(10), v1::VoidScorer{});
            que.finalize();
            auto results = que.topk();
            std::sort(results.begin(), results.end(), std::greater{});
            return results;
        }();

        REQUIRE(expected.size() == on_the_fly.size());
        REQUIRE(expected.size() == precomputed.size());
        for (size_t i = 0; i < on_the_fly.size(); ++i) {
            REQUIRE(on_the_fly[i].second == expected[i].second);
            REQUIRE(on_the_fly[i].first == Approx(expected[i].first).epsilon(0.1));
            REQUIRE(precomputed[i].second == expected[i].second);
            REQUIRE(precomputed[i].first == Approx(expected[i].first).epsilon(0.1));
        }
    }
}

TEST_CASE("DAAT OR", "[v1][integration]")
{
    auto data = IndexData<single_index,
                          v1::Index<v1::RawCursor<v1::DocId>, v1::RawCursor<v1::Frequency>>,
                          v1::Index<v1::RawCursor<v1::DocId>, v1::RawCursor<float>>>::get();
    ranked_or_query or_q(10);
    int idx = 0;
    for (auto const &q : data->queries) {
        CAPTURE(q.terms);
        CAPTURE(idx++);

        or_q(make_scored_cursors(data->v0_index, data->wdata, q), data->v0_index.num_docs());
        auto expected = or_q.topk();
        std::sort(expected.begin(), expected.end(), std::greater{});

        auto on_the_fly = [&]() {
            auto que = daat_or(
                v1::Query{q.terms}, data->v1_index, topk_queue(10), make_bm25(data->v1_index));
            que.finalize();
            auto results = que.topk();
            std::sort(results.begin(), results.end(), std::greater{});
            return results;
        }();

        auto precomputed = [&]() {
            auto que =
                daat_or(v1::Query{q.terms}, data->scored_index, topk_queue(10), v1::VoidScorer{});
            que.finalize();
            auto results = que.topk();
            std::sort(results.begin(), results.end(), std::greater{});
            return results;
        }();

        REQUIRE(expected.size() == on_the_fly.size());
        REQUIRE(expected.size() == precomputed.size());
        for (size_t i = 0; i < on_the_fly.size(); ++i) {
            REQUIRE(on_the_fly[i].second == expected[i].second);
            REQUIRE(on_the_fly[i].first == Approx(expected[i].first).epsilon(0.1));
            REQUIRE(precomputed[i].second == expected[i].second);
            REQUIRE(precomputed[i].first == Approx(expected[i].first).epsilon(0.1));
        }
    }
}
