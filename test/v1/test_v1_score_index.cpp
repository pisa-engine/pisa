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
#include "v1/types.hpp"

using pisa::v1::BlockedCursor;
using pisa::v1::BlockedReader;
using pisa::v1::BlockedWriter;
using pisa::v1::compress_binary_collection;
using pisa::v1::DocId;
using pisa::v1::Frequency;
using pisa::v1::index_runner;
using pisa::v1::IndexMetadata;
using pisa::v1::make_bm25;
using pisa::v1::RawCursor;
using pisa::v1::RawReader;
using pisa::v1::RawWriter;
using pisa::v1::TermId;

TEMPLATE_TEST_CASE("DAAT OR",
                   "[v1][integration]",
                   (IndexFixture<RawCursor<DocId>, RawCursor<Frequency>, RawCursor<std::uint8_t>>),
                   (IndexFixture<BlockedCursor<::pisa::simdbp_block, true>,
                                 BlockedCursor<::pisa::simdbp_block, false>,
                                 RawCursor<std::uint8_t>>))
{
    tbb::task_scheduler_init init(1);
    GIVEN("Index fixture (built and scored index)")
    {
        TestType fixture;
        THEN("Float max scores are correct")
        {
            auto run = v1::index_runner(
                fixture.meta(), fixture.document_reader(), fixture.frequency_reader());
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
            auto run = v1::scored_index_runner(
                fixture.meta(), fixture.document_reader(), fixture.score_reader());
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
