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

TEMPLATE_TEST_CASE("Max score join",
                   "[v1][integration]",
                   (IndexFixture<v1::RawCursor<v1::DocId>,
                                 v1::RawCursor<v1::Frequency>,
                                 v1::RawCursor<std::uint8_t>>))
{
    tbb::task_scheduler_init init(1);
    TestType fixture;

    SECTION("Zero threshold -- equivalent to union")
    {
        auto index_basename = (fixture.tmpdir().path() / "inv").string();
        auto meta = v1::IndexMetadata::from_file(fmt::format("{}.yml", index_basename));
        int idx = 0;
        for (auto& q : test_queries()) {
            CAPTURE(q.get_term_ids());
            CAPTURE(idx++);

            auto add = [](auto score, auto&& cursor, [[maybe_unused]] auto idx) {
                return score + cursor.payload();
            };
            auto run =
                v1::index_runner(meta, fixture.document_reader(), fixture.frequency_reader());
            run([&](auto&& index) {
                auto union_results = collect(v1::union_merge(
                    index.scored_cursors(gsl::make_span(q.get_term_ids()), make_bm25(index)),
                    0.0F,
                    add));
                auto maxscore_results = collect(v1::join_maxscore(
                    index.max_scored_cursors(gsl::make_span(q.get_term_ids()), make_bm25(index)),
                    0.0F,
                    Add{},
                    [](auto /* score */) { return true; }));
                REQUIRE(union_results == maxscore_results);
            });
        }
    }
}
