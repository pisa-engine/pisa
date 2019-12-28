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
#include "v1/blocked_cursor.hpp"
#include "v1/cursor/collect.hpp"
#include "v1/cursor/for_each.hpp"
#include "v1/index.hpp"
#include "v1/index_builder.hpp"
#include "v1/index_metadata.hpp"
#include "v1/scorer/bm25.hpp"
#include "v1/types.hpp"

using pisa::v1::DocId;
using pisa::v1::DocumentBlockedCursor;
using pisa::v1::DocumentBlockedReader;
using pisa::v1::DocumentBlockedWriter;
using pisa::v1::Frequency;
using pisa::v1::index_runner;
using pisa::v1::IndexMetadata;
using pisa::v1::make_bm25;
using pisa::v1::PayloadBlockedCursor;
using pisa::v1::PayloadBlockedReader;
using pisa::v1::PayloadBlockedWriter;
using pisa::v1::RawCursor;
using pisa::v1::TermId;

TEMPLATE_TEST_CASE("Score index",
                   "[v1][integration]",
                   (IndexFixture<RawCursor<DocId>, RawCursor<Frequency>, RawCursor<std::uint8_t>>),
                   (IndexFixture<DocumentBlockedCursor<::pisa::simdbp_block>,
                                 PayloadBlockedCursor<::pisa::simdbp_block>,
                                 RawCursor<std::uint8_t>>))
{
    tbb::task_scheduler_init init(1);
    GIVEN("Index fixture (built and scored index)")
    {
        TestType fixture;
        THEN("Float max scores are correct")
        {
            auto run = v1::index_runner(fixture.meta(),
                                        std::make_tuple(fixture.document_reader()),
                                        std::make_tuple(fixture.frequency_reader()));
            run([&](auto&& index) {
                for (auto term = 0; term < index.num_terms(); term += 1) {
                    CAPTURE(term);
                    auto cursor = index.max_scored_cursor(term, make_bm25(index));
                    auto precomputed_max = cursor.max_score();
                    float calculated_max = 0.0F;
                    ::pisa::v1::for_each(cursor, [&](auto&& cursor) {
                        calculated_max = std::max(cursor.payload(), calculated_max);
                    });
                    REQUIRE(precomputed_max == calculated_max);
                }
            });
        }
        THEN("Quantized max scores are correct")
        {
            auto run = v1::scored_index_runner(fixture.meta(),
                                               std::make_tuple(fixture.document_reader()),
                                               std::make_tuple(fixture.score_reader()));
            run([&](auto&& index) {
                for (auto term = 0; term < index.num_terms(); term += 1) {
                    CAPTURE(term);
                    auto cursor = index.max_scored_cursor(term, pisa::v1::VoidScorer{});
                    auto precomputed_max = cursor.max_score();
                    std::uint8_t calculated_max = 0;
                    ::pisa::v1::for_each(cursor, [&](auto&& cursor) {
                        if (cursor.payload() > calculated_max) {
                            calculated_max = cursor.payload();
                        }
                    });
                    REQUIRE(precomputed_max == calculated_max);
                }
            });
        }
    }
}

TEMPLATE_TEST_CASE("Construct max-score lists",
                   "[v1][integration]",
                   (IndexFixture<RawCursor<DocId>, RawCursor<Frequency>, RawCursor<std::uint8_t>>),
                   (IndexFixture<DocumentBlockedCursor<::pisa::simdbp_block>,
                                 PayloadBlockedCursor<::pisa::simdbp_block>,
                                 RawCursor<std::uint8_t>>))
{
    tbb::task_scheduler_init init(1);
    GIVEN("Index fixture (built and (max) scored index)")
    {
        TestType fixture;
        THEN("Float max scores are correct")
        {
            auto run = v1::index_runner(fixture.meta(),
                                        std::make_tuple(fixture.document_reader()),
                                        std::make_tuple(fixture.frequency_reader()));
            run([&](auto&& index) {
                for (auto term = 0; term < index.num_terms(); term += 1) {
                    CAPTURE(term);
                    auto cursor = index.block_max_scored_cursor(term, make_bm25(index));
                    auto term_max_score = 0.0F;
                    while (not cursor.empty()) {
                        auto max_score = 0.0F;
                        auto block_max_score = cursor.block_max_score(*cursor);
                        for (auto idx = 0; idx < 5 && not cursor.empty(); ++idx) {
                            REQUIRE(cursor.block_max_score(*cursor) == block_max_score);
                            if (auto score = cursor.payload(); score > max_score) {
                                max_score = score;
                            }
                            cursor.advance();
                        }
                        if (max_score > term_max_score) {
                            term_max_score = max_score;
                        }
                        REQUIRE(max_score == block_max_score);
                    }
                    REQUIRE(term_max_score == cursor.max_score());
                }
            });
        }
    }
}
