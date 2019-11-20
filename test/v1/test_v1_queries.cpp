#define CATCH_CONFIG_MAIN
#include "catch2/catch.hpp"

#include <string>

#include <boost/iostreams/device/back_inserter.hpp>
#include <boost/iostreams/stream.hpp>
#include <tbb/task_scheduler_init.h>

#include "../temporary_directory.hpp"
#include "accumulator/lazy_accumulator.hpp"
#include "cursor/block_max_scored_cursor.hpp"
#include "cursor/max_scored_cursor.hpp"
#include "cursor/scored_cursor.hpp"
#include "index_fixture.hpp"
#include "index_types.hpp"
#include "io.hpp"
#include "pisa_config.hpp"
#include "query/queries.hpp"
#include "v1/blocked_cursor.hpp"
#include "v1/cursor/collect.hpp"
#include "v1/cursor_intersection.hpp"
#include "v1/cursor_traits.hpp"
#include "v1/cursor_union.hpp"
#include "v1/index.hpp"
#include "v1/index_builder.hpp"
#include "v1/maxscore.hpp"
#include "v1/posting_builder.hpp"
#include "v1/posting_format_header.hpp"
#include "v1/query.hpp"
#include "v1/score_index.hpp"
#include "v1/scorer/bm25.hpp"
#include "v1/types.hpp"
#include "v1/union_lookup.hpp"

namespace v1 = pisa::v1;
using namespace pisa;

static constexpr auto RELATIVE_ERROR = 0.1F;

template <typename v0_Index, typename v1_Index, typename ScoredIndex>
struct IndexData {

    static std::unique_ptr<IndexData> data;

    IndexData()
        : collection(PISA_SOURCE_DIR "/test/test_data/test_collection"),
          document_sizes(PISA_SOURCE_DIR "/test/test_data/test_collection.sizes"),
          wdata(document_sizes.begin()->begin(),
                collection.num_docs(),
                collection,
                BlockSize(FixedBlock()))

    {
        typename v0_Index::builder builder(collection.num_docs(), params);
        for (auto const& plist : collection) {
            uint64_t freqs_sum =
                std::accumulate(plist.freqs.begin(), plist.freqs.end(), uint64_t(0));
            builder.add_posting_list(
                plist.docs.size(), plist.docs.begin(), plist.freqs.begin(), freqs_sum);
        }
        builder.build(v0_index);

        term_id_vec q;
        std::ifstream qfile(PISA_SOURCE_DIR "/test/test_data/queries");
        auto push_query = [&](std::string const& query_line) {
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
    std::vector<Query> queries;
    std::vector<float> thresholds;
    wand_data<wand_data_raw> wdata;
};

/// Inefficient, do not use in production code.
[[nodiscard]] auto approximate_order(std::pair<float, std::uint32_t> lhs,
                                     std::pair<float, std::uint32_t> rhs) -> bool
{
    return std::make_pair(fmt::format("{:0.4f}", lhs.first), lhs.second)
           < std::make_pair(fmt::format("{:0.4f}", rhs.first), rhs.second);
}

/// Inefficient, do not use in production code.
[[nodiscard]] auto approximate_order_f(float lhs, float rhs) -> bool
{
    return fmt::format("{:0.4f}", lhs) < fmt::format("{:0.4f}", rhs);
}

template <typename v0_Index, typename v1_Index, typename ScoredIndex>
std::unique_ptr<IndexData<v0_Index, v1_Index, ScoredIndex>>
    IndexData<v0_Index, v1_Index, ScoredIndex>::data = nullptr;

TEMPLATE_TEST_CASE("Query",
                   "[v1][integration]",
                   (IndexFixture<v1::RawCursor<v1::DocId>,
                                 v1::RawCursor<v1::Frequency>,
                                 v1::RawCursor<std::uint8_t>>),
                   (IndexFixture<v1::BlockedCursor<::pisa::simdbp_block, true>,
                                 v1::BlockedCursor<::pisa::simdbp_block, false>,
                                 v1::RawCursor<std::uint8_t>>))
{
    tbb::task_scheduler_init init(1);
    auto data = IndexData<single_index,
                          v1::Index<v1::RawCursor<v1::DocId>, v1::RawCursor<v1::Frequency>>,
                          v1::Index<v1::RawCursor<v1::DocId>, v1::RawCursor<float>>>::get();
    TestType fixture;
    auto algorithm = GENERATE(std::string("daat_or"), std::string("maxscore"));
    CAPTURE(algorithm);
    auto index_basename = (fixture.tmpdir().path() / "inv").string();
    auto meta = v1::IndexMetadata::from_file(fmt::format("{}.yml", index_basename));
    ranked_or_query or_q(10);
    auto run_query = [](std::string const& name, auto query, auto&& index, auto scorer) {
        if (name == "daat_or") {
            return daat_or(query, index, topk_queue(10), scorer);
        }
        if (name == "maxscore") {
            return maxscore(query, index, topk_queue(10), scorer);
        }
        std::abort();
    };
    int idx = 0;
    for (auto& q : test_queries()) {
        std::sort(q.terms.begin(), q.terms.end());
        q.terms.erase(std::unique(q.terms.begin(), q.terms.end()), q.terms.end());
        CAPTURE(q.terms);
        CAPTURE(idx++);

        or_q(make_scored_cursors(data->v0_index, data->wdata, q), data->v0_index.num_docs());
        auto expected = or_q.topk();
        std::sort(expected.begin(), expected.end(), approximate_order);

        auto on_the_fly = [&]() {
            auto run =
                v1::index_runner(meta, fixture.document_reader(), fixture.frequency_reader());
            std::vector<typename topk_queue::entry_type> results;
            run([&](auto&& index) {
                auto que = run_query(algorithm, v1::Query{q.terms}, index, make_bm25(index));
                que.finalize();
                results = que.topk();
                std::sort(results.begin(), results.end(), approximate_order);
            });
            return results;
        }();

        REQUIRE(expected.size() == on_the_fly.size());
        for (size_t i = 0; i < on_the_fly.size(); ++i) {
            REQUIRE(on_the_fly[i].second == expected[i].second);
            REQUIRE(on_the_fly[i].first == Approx(expected[i].first).epsilon(RELATIVE_ERROR));
        }

        auto precomputed = [&]() {
            auto run =
                v1::scored_index_runner(meta, fixture.document_reader(), fixture.score_reader());
            std::vector<typename topk_queue::entry_type> results;
            run([&](auto&& index) {
                auto que = run_query(algorithm, v1::Query{q.terms}, index, v1::VoidScorer{});
                que.finalize();
                results = que.topk();
            });
            // Remove the tail that might be different due to quantization error.
            // Note that `precomputed` will have summed quantized score, while the
            // vector we compare to will have quantized sum---that's why whe remove anything
            // that's withing 2 of the last result.
            // auto last_score = results.back().first;
            // results.erase(std::remove_if(
            //                  results.begin(),
            //                  results.end(),
            //                  [last_score](auto&& entry) { return entry.first <= last_score + 3;
            //                  }),
            //              results.end());
            // results.resize(5);
            // std::sort(results.begin(), results.end(), [](auto&& lhs, auto&& rhs) {
            //    return lhs.second < rhs.second;
            //});
            return results;
        }();

        constexpr float max_partial_score = 16.5724F;
        auto quantizer = [&](float score) {
            return static_cast<std::uint8_t>(score * std::numeric_limits<std::uint8_t>::max()
                                             / max_partial_score);
        };

        auto expected_quantized = expected;
        std::sort(expected_quantized.begin(), expected_quantized.end(), [](auto&& lhs, auto&& rhs) {
            return lhs.first > rhs.first;
        });
        for (auto& v : expected_quantized) {
            v.first = quantizer(v.first);
        }

        // TODO(michal): test the quantized results

        // expected_quantized.resize(precomputed.size());
        // std::sort(expected_quantized.begin(), expected_quantized.end(), [](auto&& lhs, auto&&
        // rhs) {
        //    return lhs.second < rhs.second;
        //});

        // for (size_t i = 0; i < precomputed.size(); ++i) {
        //    std::cerr << fmt::format("{}, {:f} -- {}, {:f}\n",
        //                             precomputed[i].second,
        //                             precomputed[i].first,
        //                             expected_quantized[i].second,
        //                             expected_quantized[i].first);
        //}

        // for (size_t i = 0; i < precomputed.size(); ++i) {
        //    REQUIRE(std::abs(precomputed[i].first - expected_quantized[i].first)
        //            <= static_cast<float>(q.terms.size()));
        //}
    }
}

TEMPLATE_TEST_CASE("UnionLookup",
                   "[v1][integration]",
                   (IndexFixture<v1::RawCursor<v1::DocId>,
                                 v1::RawCursor<v1::Frequency>,
                                 v1::RawCursor<std::uint8_t>>),
                   (IndexFixture<v1::BlockedCursor<::pisa::simdbp_block, true>,
                                 v1::BlockedCursor<::pisa::simdbp_block, false>,
                                 v1::RawCursor<std::uint8_t>>))
{
    tbb::task_scheduler_init init(1);
    auto data = IndexData<single_index,
                          v1::Index<v1::RawCursor<v1::DocId>, v1::RawCursor<v1::Frequency>>,
                          v1::Index<v1::RawCursor<v1::DocId>, v1::RawCursor<float>>>::get();
    TestType fixture;
    auto index_basename = (fixture.tmpdir().path() / "inv").string();
    auto meta = v1::IndexMetadata::from_file(fmt::format("{}.yml", index_basename));
    ranked_or_query or_q(10);
    int idx = 0;
    for (auto& q : test_queries()) {
        CAPTURE(q.terms);
        CAPTURE(idx++);

        or_q(make_scored_cursors(data->v0_index, data->wdata, q), data->v0_index.num_docs());
        auto expected = or_q.topk();
        std::sort(expected.begin(), expected.end(), std::greater{});

        auto on_the_fly = [&]() {
            auto run =
                v1::index_runner(meta, fixture.document_reader(), fixture.frequency_reader());
            std::vector<typename topk_queue::entry_type> results;
            run([&](auto&& index) {
                std::vector<std::size_t> unigrams(q.terms.size());
                std::iota(unigrams.begin(), unigrams.end(), 0);
                auto que = v1::union_lookup(v1::Query{q.terms},
                                            index,
                                            topk_queue(10),
                                            make_bm25(index),
                                            std::move(unigrams),
                                            {});
                que.finalize();
                results = que.topk();
                std::sort(results.begin(), results.end(), std::greater{});
            });
            return results;
        }();

        // auto precomputed = [&]() {
        //    auto run =
        //        v1::scored_index_runner(meta, fixture.document_reader(), fixture.score_reader());
        //    std::vector<typename topk_queue::entry_type> results;
        //    run([&](auto&& index) {
        //        auto que = daat_or(v1::Query{q.terms}, index, topk_queue(10), v1::VoidScorer{});
        //        que.finalize();
        //        results = que.topk();
        //        std::sort(results.begin(), results.end(), std::greater{});
        //    });
        //    return results;
        //}();

        REQUIRE(expected.size() == on_the_fly.size());
        // REQUIRE(expected.size() == precomputed.size());
        for (size_t i = 0; i < on_the_fly.size(); ++i) {
            REQUIRE(on_the_fly[i].second == expected[i].second);
            REQUIRE(on_the_fly[i].first == Approx(expected[i].first).epsilon(RELATIVE_ERROR));
            // REQUIRE(precomputed[i].second == expected[i].second);
            // REQUIRE(precomputed[i].first == Approx(expected[i].first).epsilon(RELATIVE_ERROR));
        }
    }
}
