#define CATCH_CONFIG_MAIN
#include "catch2/catch.hpp"

#include <string>

#include <boost/iostreams/device/back_inserter.hpp>
#include <boost/iostreams/stream.hpp>
#include <tbb/task_scheduler_init.h>

#include "../temporary_directory.hpp"
#include "codec/simdbp.hpp"
#include "pisa_config.hpp"
#include "v1/blocked_cursor.hpp"
#include "v1/cursor/collect.hpp"
#include "v1/index.hpp"
#include "v1/index_builder.hpp"
#include "v1/index_metadata.hpp"
#include "v1/io.hpp"
#include "v1/posting_builder.hpp"
#include "v1/types.hpp"

using pisa::v1::DocId;
using pisa::v1::DocumentPayloadCursor;
using pisa::v1::Frequency;
using pisa::v1::RawCursor;
using pisa::v1::TermId;

TEST_CASE("Document-payload cursor", "[v1][unit]")
{
    GIVEN("Document-payload cursor")
    {
        std::vector<std::uint32_t> documents{4, 0, 1, 5, 7};
        std::vector<std::uint32_t> frequencies{4, 2, 2, 1, 6};
        auto cursor = DocumentPayloadCursor<RawCursor<std::uint32_t>, RawCursor<std::uint32_t>>(
            RawCursor<std::uint32_t>(gsl::as_bytes(gsl::make_span(documents))),
            RawCursor<std::uint32_t>(gsl::as_bytes(gsl::make_span(frequencies))));

        WHEN("Collected to document and frequency vectors")
        {
            std::vector<std::uint32_t> collected_documents;
            std::vector<std::uint32_t> collected_frequencies;
            for_each(cursor, [&](auto&& cursor) {
                collected_documents.push_back(cursor.value());
                collected_frequencies.push_back(cursor.payload());
            });
            THEN("Vector equals to expected")
            {
                REQUIRE(collected_documents == std::vector<std::uint32_t>{0, 1, 5, 7});
                REQUIRE(collected_frequencies == std::vector<std::uint32_t>{2, 2, 1, 6});
            }
        }

        WHEN("Stepped with advance_to_pos")
        {
            cursor.advance_to_position(0);
            REQUIRE(cursor.value() == 0);
            REQUIRE(cursor.payload() == 2);
            cursor.advance_to_position(1);
            REQUIRE(cursor.value() == 1);
            REQUIRE(cursor.payload() == 2);
            cursor.advance_to_position(2);
            REQUIRE(cursor.value() == 5);
            REQUIRE(cursor.payload() == 1);
            cursor.advance_to_position(3);
            REQUIRE(cursor.value() == 7);
            REQUIRE(cursor.payload() == 6);
        }

        WHEN("Advanced to 1")
        {
            cursor.advance_to_position(1);
            REQUIRE(cursor.value() == 1);
            REQUIRE(cursor.payload() == 2);
        }
        WHEN("Advanced to 2")
        {
            cursor.advance_to_position(2);
            REQUIRE(cursor.value() == 5);
            REQUIRE(cursor.payload() == 1);
        }
        WHEN("Advanced to 3")
        {
            cursor.advance_to_position(3);
            REQUIRE(cursor.value() == 7);
            REQUIRE(cursor.payload() == 6);
        }
    }
}
