#define CATCH_CONFIG_MAIN
#include <catch2/catch.hpp>

#include <algorithm>
#include <vector>

#include <range/v3/algorithm/sort.hpp>
#include <range/v3/numeric/accumulate.hpp>
#include <range/v3/view/enumerate.hpp>
#include <rapidcheck.h>
#include <tbb/task_scheduler_init.h>

#include "index_fixture.hpp"
#include "topk_queue.hpp"
#include "v1/cursor/collect.hpp"
#include "v1/cursor_accumulator.hpp"
#include "v1/index_metadata.hpp"
#include "v1/maxscore.hpp"
#include "v1/raw_cursor.hpp"
#include "v1/scorer/bm25.hpp"
#include "v1/union_lookup.hpp"

using pisa::v1::BM25;
using pisa::v1::collect_payloads;
using pisa::v1::collect_with_payload;
using pisa::v1::DocId;
using pisa::v1::Frequency;
using pisa::v1::index_runner;
using pisa::v1::InspectLookupUnion;
using pisa::v1::InspectLookupUnionEaat;
using pisa::v1::InspectMaxScore;
using pisa::v1::InspectUnionLookup;
using pisa::v1::InspectUnionLookupPlus;
using pisa::v1::join_union_lookup;
using pisa::v1::lookup_union;
using pisa::v1::lookup_union_eaat;
using pisa::v1::maxscore_partition;
using pisa::v1::RawCursor;

template <typename T>
void test_write(T&& result)
{
    std::ostringstream os;
    result.write(os);
    REQUIRE(fmt::format("{}\t{}\t{}\t{}\t{}",
                        result.postings(),
                        result.documents(),
                        result.lookups(),
                        result.inserts(),
                        result.essentials())
            == os.str());
}

template <typename T>
void test_write_partitioned(T&& result)
{
    std::ostringstream os;
    result.write(os);
    REQUIRE(fmt::format("{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
                        result.sum.postings(),
                        result.sum.documents(),
                        result.sum.lookups(),
                        result.sum.inserts(),
                        result.sum.essentials(),
                        result.first.postings(),
                        result.first.documents(),
                        result.first.lookups(),
                        result.first.inserts(),
                        result.first.essentials(),
                        result.second.postings(),
                        result.second.documents(),
                        result.second.lookups(),
                        result.second.inserts(),
                        result.second.essentials())
            == os.str());
}

TEST_CASE("UnionLookup statistics", "[union-lookup][v1][unit]")
{
    tbb::task_scheduler_init init;
    IndexFixture<RawCursor<DocId>, RawCursor<Frequency>, RawCursor<std::uint8_t>> fixture;
    index_runner(fixture.meta(),
                 std::make_tuple(fixture.document_reader()),
                 std::make_tuple(fixture.frequency_reader()))([&](auto&& index) {
        auto union_lookup_inspect = InspectUnionLookup(index, make_bm25(index));
        auto union_lookup_plus_inspect = InspectUnionLookupPlus(index, make_bm25(index));
        auto lookup_union_inspect = InspectLookupUnion(index, make_bm25(index));
        auto lookup_union_eaat_inspect = InspectLookupUnionEaat(index, make_bm25(index));
        auto queries = test_queries();
        auto const intersections =
            pisa::v1::read_intersections(PISA_SOURCE_DIR "/test/test_data/top10_selections");
        for (auto&& [idx, q] : ranges::views::enumerate(queries)) {
            // if (idx == 230 || idx == 272 || idx == 380) {
            //    // Skipping these because of the false positives caused by floating point
            //    precision. continue;
            //}
            if (q.get_term_ids().size() > 8) {
                continue;
            }
            auto heap = maxscore(q, index, ::pisa::topk_queue(10), make_bm25(index));
            q.selections(intersections[idx]);
            q.threshold(heap.topk().back().first);
            CAPTURE(q.get_term_ids());
            CAPTURE(intersections[idx]);
            CAPTURE(q.get_threshold());
            CAPTURE(idx);

            auto ul = union_lookup_inspect(q);
            auto ulp = union_lookup_plus_inspect(q);
            auto lu = lookup_union_inspect(q);
            auto lue = lookup_union_eaat_inspect(q);
            test_write(ul);
            test_write(ulp);
            test_write_partitioned(lu);
            test_write_partitioned(lue);

            CHECK(ul.documents() == ulp.documents());
            CHECK(ul.postings() == ulp.postings());

            // +2 because of the false positives caused by floating point
            CHECK(ul.lookups() + 2 >= ulp.lookups());

            CAPTURE(ulp.lookups());
            CAPTURE(ul.lookups());
            CAPTURE(lu.first.lookups());
            CAPTURE(lu.second.lookups());

            CAPTURE(ul.essentials());
            CAPTURE(lu.first.essentials());
            CAPTURE(lu.second.essentials());

            CHECK(lu.first.lookups() + lu.second.lookups() == lu.sum.lookups());
            CHECK(lue.first.lookups() + lue.second.lookups() == lue.sum.lookups());
            CHECK(ul.postings() == lu.sum.postings());
            CHECK(ul.postings() == lue.sum.postings());

            // +3 because of the false positives caused by floating point
            CHECK(ulp.lookups() <= lu.sum.lookups() + 3);
            CHECK(ulp.lookups() <= lue.sum.lookups() + 3);
        }
        auto ul = union_lookup_inspect.mean();
        auto ulp = union_lookup_plus_inspect.mean();
        auto lu = lookup_union_inspect.mean();
        auto lue = lookup_union_inspect.mean();
        CHECK(ul.documents() == ulp.documents());
        CHECK(ul.postings() == ulp.postings());
        CHECK(ul.lookups() >= ulp.lookups());
        CHECK(ul.postings() == lu.first.postings() + lu.second.postings());
        CHECK(ul.postings() == lue.first.postings() + lue.second.postings());
        CHECK(ulp.lookups() <= lu.first.lookups() + lu.second.lookups());
        CHECK(ulp.lookups() <= lue.first.lookups() + lue.second.lookups());
        CHECK(lu.first.lookups() + lu.second.lookups() == lu.sum.lookups());
        CHECK(lue.first.lookups() + lue.second.lookups() == lue.sum.lookups());
    });
}
