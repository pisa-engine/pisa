#define CATCH_CONFIG_MAIN
#include "catch2/catch.hpp"

#include <string>
#include <tuple>

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
#include "v1/cursor_accumulator.hpp"
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

static constexpr auto RELATIVE_ERROR = 0.1F;

TEMPLATE_TEST_CASE("Bigram v intersection",
                   "[v1][integration]",
                   (IndexFixture<v1::RawCursor<v1::DocId>,
                                 v1::RawCursor<v1::Frequency>,
                                 v1::RawCursor<std::uint8_t>>),
                   (IndexFixture<v1::DocumentBlockedCursor<::pisa::simdbp_block>,
                                 v1::PayloadBlockedCursor<::pisa::simdbp_block>,
                                 v1::RawCursor<std::uint8_t>>))
{
    tbb::task_scheduler_init init(1);
    TestType fixture;
    auto index_basename = (fixture.tmpdir().path() / "inv").string();
    auto meta = v1::IndexMetadata::from_file(fmt::format("{}.yml", index_basename));
    int idx = 0;
    for (auto& q : test_queries()) {
        CAPTURE(q.get_term_ids());
        CAPTURE(idx++);

        auto run = v1::index_runner(meta,
                                    std::make_tuple(fixture.document_reader()),
                                    std::make_tuple(fixture.frequency_reader()));
        std::vector<typename pisa::topk_queue::entry_type> results;
        run([&](auto&& index) {
            for (auto left = 0; left < q.get_term_ids().size(); left += 1) {
                for (auto right = left + 1; right < q.get_term_ids().size(); right += 1) {
                    CAPTURE(q.get_term_ids()[left]);
                    CAPTURE(q.get_term_ids()[right]);
                    auto left_cursor = index.cursor(q.get_term_ids()[left]);
                    auto right_cursor = index.cursor(q.get_term_ids()[right]);
                    auto intersection = v1::intersect({left_cursor, right_cursor},
                                                      std::array<v1::Frequency, 2>{0, 0},
                                                      [](auto& acc, auto&& cursor, auto idx) {
                                                          gsl::at(acc, idx) = cursor.payload();
                                                          return acc;
                                                      });
                    if (not intersection.empty()) {
                        auto bigram_cursor =
                            index.bigram_cursor(q.get_term_ids()[left], q.get_term_ids()[right])
                                .value();
                        std::vector<v1::DocId> bigram_documents;
                        std::vector<v1::Frequency> bigram_frequencies_0;
                        std::vector<v1::Frequency> bigram_frequencies_1;
                        v1::for_each(bigram_cursor, [&](auto&& cursor) {
                            bigram_documents.push_back(*cursor);
                            auto payload = cursor.payload();
                            bigram_frequencies_0.push_back(std::get<0>(payload));
                            bigram_frequencies_1.push_back(std::get<1>(payload));
                        });
                        std::vector<v1::DocId> intersection_documents;
                        std::vector<v1::Frequency> intersection_frequencies_0;
                        std::vector<v1::Frequency> intersection_frequencies_1;
                        v1::for_each(intersection, [&](auto&& cursor) {
                            intersection_documents.push_back(*cursor);
                            auto payload = cursor.payload();
                            intersection_frequencies_0.push_back(std::get<0>(payload));
                            intersection_frequencies_1.push_back(std::get<1>(payload));
                        });
                        CHECK(bigram_documents == intersection_documents);
                        CHECK(bigram_frequencies_0 == intersection_frequencies_0);
                        REQUIRE(bigram_frequencies_1 == intersection_frequencies_1);
                    }
                }
            }
        });
    }
}

TEMPLATE_TEST_CASE("Scored pair index v. intersection",
                   "[v1][integration]",
                   (IndexFixture<v1::RawCursor<v1::DocId>,
                                 v1::RawCursor<v1::Frequency>,
                                 v1::RawCursor<std::uint8_t>>),
                   (IndexFixture<v1::DocumentBlockedCursor<::pisa::simdbp_block>,
                                 v1::PayloadBlockedCursor<::pisa::simdbp_block>,
                                 v1::RawCursor<std::uint8_t>>))
{
    tbb::task_scheduler_init init(1);
    TestType fixture;
    auto index_basename = (fixture.tmpdir().path() / "inv").string();
    auto meta = v1::IndexMetadata::from_file(fmt::format("{}.yml", index_basename));
    int idx = 0;
    for (auto& q : test_queries()) {
        CAPTURE(q.get_term_ids());
        CAPTURE(idx++);

        auto run = v1::scored_index_runner(meta,
                                           std::make_tuple(fixture.document_reader()),
                                           std::make_tuple(fixture.score_reader()));
        std::vector<typename pisa::topk_queue::entry_type> results;
        run([&](auto&& index) {
            for (auto left = 0; left < q.get_term_ids().size(); left += 1) {
                for (auto right = left + 1; right < q.get_term_ids().size(); right += 1) {
                    CAPTURE(q.get_term_ids()[left]);
                    CAPTURE(q.get_term_ids()[right]);
                    auto left_cursor = index.cursor(q.get_term_ids()[left]);
                    auto right_cursor = index.cursor(q.get_term_ids()[right]);
                    auto intersection = v1::intersect({left_cursor, right_cursor},
                                                      std::array<float, 2>{0.0F, 0.0F},
                                                      [](auto& acc, auto&& cursor, auto idx) {
                                                          gsl::at(acc, idx) = cursor.payload();
                                                          return acc;
                                                      });
                    if (not intersection.empty()) {
                        auto bigram_cursor =
                            index.bigram_cursor(q.get_term_ids()[left], q.get_term_ids()[right])
                                .value();
                        std::vector<v1::DocId> bigram_documents;
                        std::vector<float> bigram_scores_0;
                        std::vector<float> bigram_scores_1;
                        v1::for_each(bigram_cursor, [&](auto&& cursor) {
                            bigram_documents.push_back(*cursor);
                            auto payload = cursor.payload();
                            bigram_scores_0.push_back(std::get<0>(payload));
                            bigram_scores_1.push_back(std::get<1>(payload));
                        });
                        std::vector<v1::DocId> intersection_documents;
                        std::vector<float> intersection_scores_0;
                        std::vector<float> intersection_scores_1;
                        v1::for_each(intersection, [&](auto&& cursor) {
                            intersection_documents.push_back(*cursor);
                            auto payload = cursor.payload();
                            intersection_scores_0.push_back(std::get<0>(payload));
                            intersection_scores_1.push_back(std::get<1>(payload));
                        });
                        CHECK(bigram_documents == intersection_documents);
                        CHECK(bigram_scores_0 == intersection_scores_0);
                        REQUIRE(bigram_scores_1 == intersection_scores_1);
                    }
                }
            }
        });
    }
}
