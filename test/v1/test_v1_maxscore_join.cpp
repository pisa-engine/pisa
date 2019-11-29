#define CATCH_CONFIG_MAIN
#include "catch2/catch.hpp"

#include <string>

#include <boost/iostreams/device/back_inserter.hpp>
#include <boost/iostreams/stream.hpp>
#include <tbb/task_scheduler_init.h>

#include "../temporary_directory.hpp"
#include "codec/simdbp.hpp"
#include "index_fixture.hpp"
#include "pisa_config.hpp"
#include "topk_queue.hpp"
#include "v1/blocked_cursor.hpp"
#include "v1/cursor/collect.hpp"
#include "v1/cursor_accumulator.hpp"
#include "v1/index.hpp"
#include "v1/index_builder.hpp"
#include "v1/index_metadata.hpp"
#include "v1/io.hpp"
#include "v1/maxscore.hpp"
#include "v1/posting_builder.hpp"
#include "v1/types.hpp"

using pisa::v1::BlockedReader;
using pisa::v1::BlockedWriter;
using pisa::v1::collect;
using pisa::v1::DocId;
using pisa::v1::Frequency;
using pisa::v1::IndexRunner;
using pisa::v1::join_maxscore;
using pisa::v1::PostingBuilder;
using pisa::v1::RawReader;
using pisa::v1::read_sizes;
using pisa::v1::TermId;
using pisa::v1::accumulate::Add;

TEMPLATE_TEST_CASE("",
                   "[v1][integration]",
                   (IndexFixture<v1::RawCursor<v1::DocId>,
                                 v1::RawCursor<v1::Frequency>,
                                 v1::RawCursor<std::uint8_t>>))
//(IndexFixture<v1::BlockedCursor<::pisa::simdbp_block, true>,
//              v1::BlockedCursor<::pisa::simdbp_block, false>,
//              v1::RawCursor<std::uint8_t>>))
{
    tbb::task_scheduler_init init(1);
    TestType fixture;

    SECTION("Zero threshold -- equivalent to union")
    {
        auto index_basename = (fixture.tmpdir().path() / "inv").string();
        auto meta = v1::IndexMetadata::from_file(fmt::format("{}.yml", index_basename));
        // auto run_query = [](std::string const& name, auto query, auto&& index, auto scorer) {
        //    if (name == "daat_or") {
        //        return daat_or(query, index, topk_queue(10), scorer);
        //    }
        //    if (name == "maxscore") {
        //        return maxscore(query, index, topk_queue(10), scorer);
        //    }
        //    std::abort();
        //};
        int idx = 0;
        for (auto& q : test_queries()) {
            CAPTURE(q.get_term_ids());
            CAPTURE(idx++);

            auto run =
                v1::index_runner(meta, fixture.document_reader(), fixture.frequency_reader());
            run([&](auto&& index) {
                auto union_results = collect(v1::union_merge(
                    index.scored_cursors(gsl::make_span(q.get_term_ids()), make_bm25(index)),
                    0.0F,
                    Add{}));
                auto maxscore_results = collect(v1::join_maxscore(
                    index.max_scored_cursors(gsl::make_span(q.get_term_ids()), make_bm25(index)),
                    0.0F,
                    Add{},
                    [](auto /* score */) { return true; }));
                REQUIRE(union_results == maxscore_results);
            });

            run([&](auto&& index) {
                auto union_results = collect_with_payload(v1::union_merge(
                    index.scored_cursors(gsl::make_span(q.get_term_ids()), make_bm25(index)),
                    0.0F,
                    Add{}));
                union_results.erase(std::remove_if(union_results.begin(),
                                                   union_results.end(),
                                                   [](auto score) { return score.second <= 5.0F; }),
                                    union_results.end());
                auto maxscore_results = collect_with_payload(v1::join_maxscore(
                    index.max_scored_cursors(gsl::make_span(q.get_term_ids()), make_bm25(index)),
                    0.0F,
                    Add{},
                    [](auto score) { return score > 5.0F; }));
                REQUIRE(union_results.size() == maxscore_results.size());
                for (size_t i = 0; i < union_results.size(); ++i) {
                    CAPTURE(i);
                    REQUIRE(union_results[i].first == union_results[i].first);
                    REQUIRE(union_results[i].second
                            == Approx(union_results[i].second).epsilon(0.01));
                    // REQUIRE(precomputed[i].second == expected[i].second);
                    // REQUIRE(precomputed[i].first ==
                    // Approx(expected[i].first).epsilon(RELATIVE_ERROR));
                }
            });

            //    // auto precomputed = [&]() {
            //    //     auto run =
            //    //         v1::scored_index_runner(meta, fixture.document_reader(),
            //    //         fixture.score_reader());
            //    //     std::vector<typename topk_queue::entry_type> results;
            //    //     run([&](auto&& index) {
            //    //         // auto que = run_query(algorithm, v1::Query{q.terms}, index,
            //    //         v1::VoidScorer{}); auto que = daat_or(v1::Query{q.terms}, index,
            //    //         topk_queue(10),v1::VoidScorer{}); que.finalize(); results = que.topk();
            //    //         std::sort(results.begin(), results.end(), std::greater{});
            //    //     });
            //    //     return results;
            //    // }();

            //    REQUIRE(expected.size() == on_the_fly.size());
            //    // REQUIRE(expected.size() == precomputed.size());
            //    for (size_t i = 0; i < on_the_fly.size(); ++i) {
            //        REQUIRE(on_the_fly[i].second == expected[i].second);
            //        REQUIRE(on_the_fly[i].first ==
            //        Approx(expected[i].first).epsilon(RELATIVE_ERROR));
            //        // REQUIRE(precomputed[i].second == expected[i].second);
            //        // REQUIRE(precomputed[i].first ==
            //        // Approx(expected[i].first).epsilon(RELATIVE_ERROR));
            //    }
        }
    }
}
