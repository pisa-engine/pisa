#include <fmt/format.h>
#include <iostream>

template <typename T1, typename T2>
std::ostream& operator<<(std::ostream& os, std::pair<T1, T2> const& p)
{
    os << fmt::format("({}, {})", p.first, p.second);
    return os;
}

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
#include "v1/union_lookup_join.hpp"

using pisa::v1::collect_payloads;
using pisa::v1::collect_with_payload;
using pisa::v1::DocId;
using pisa::v1::Frequency;
using pisa::v1::index_runner;
using pisa::v1::join_union_lookup;
using pisa::v1::maxscore_partition;
using pisa::v1::RawCursor;
using pisa::v1::accumulators::Add;

TEST_CASE("Maxscore partition", "[maxscore][v1][unit]")
{
    rc::check("paritition vector of max scores according to maxscore", []() {
        auto max_scores = *rc::gen::nonEmpty<std::vector<float>>();
        ranges::sort(max_scores);
        float total_sum = ranges::accumulate(max_scores, 0.0F);
        auto threshold =
            *rc::gen::suchThat<float>([=](float x) { return x >= 0.0 && x < total_sum; });
        auto [non_essential, essential] =
            maxscore_partition(gsl::make_span(max_scores), threshold, [](auto&& x) { return x; });
        auto non_essential_sum = ranges::accumulate(non_essential, 0.0F);
        auto first_essential = essential.empty() ? 0.0 : essential[0];
        REQUIRE(non_essential_sum <= Approx(threshold));
        REQUIRE(non_essential_sum + first_essential >= threshold);
    });
}

struct InspectMock {
    std::size_t documents = 0;
    std::size_t postings = 0;
    std::size_t lookups = 0;

    void document() { documents += 1; }
    void posting() { postings += 1; }
    void lookup() { lookups += 1; }
};

TEST_CASE("UnionLookupJoin v Union", "[union-lookup][v1][unit]")
{
    tbb::task_scheduler_init init;
    IndexFixture<RawCursor<DocId>, RawCursor<Frequency>, RawCursor<std::uint8_t>> fixture;

    auto result_order = [](auto&& lhs, auto&& rhs) {
        if (lhs.second == rhs.second) {
            return lhs.first > rhs.first;
        }
        return lhs.second > rhs.second;
    };

    auto add = [](auto score, auto&& cursor, [[maybe_unused]] auto idx) {
        return score + cursor.payload();
    };

    index_runner(fixture.meta(),
                 std::make_tuple(fixture.document_reader()),
                 std::make_tuple(fixture.frequency_reader()))([&](auto&& index) {
        auto queries = test_queries();
        for (auto&& [idx, q] : ranges::views::enumerate(queries)) {
            CAPTURE(q.get_term_ids());
            CAPTURE(idx);

            auto term_ids = gsl::make_span(q.get_term_ids());

            auto union_results = collect_with_payload(
                v1::union_merge(index.scored_cursors(term_ids, make_bm25(index)), 0.0F, add));
            std::sort(union_results.begin(), union_results.end(), result_order);
            std::size_t num_results = std::min(union_results.size(), 10UL);
            if (num_results == 0) {
                continue;
            }
            float threshold = std::next(union_results.begin(), num_results - 1)->second;

            auto cursors = index.max_scored_cursors(term_ids, make_bm25(index));
            auto [non_essential, essential] =
                maxscore_partition(gsl::make_span(cursors), threshold);
            CAPTURE(non_essential.size());
            CAPTURE(essential.size());

            InspectMock inspect;
            auto ul_results = collect_with_payload(join_union_lookup(
                essential,
                non_essential,
                0.0F,
                Add{},
                [=](auto score) { return score >= threshold; },
                &inspect));
            std::sort(ul_results.begin(), ul_results.end(), result_order);
            REQUIRE(ul_results.size() >= num_results);
            union_results.erase(std::next(union_results.begin(), num_results), union_results.end());
            ul_results.erase(std::next(ul_results.begin(), num_results), ul_results.end());
            for (auto pos = 0; pos < num_results; pos++) {
                CAPTURE(pos);
                REQUIRE(union_results[pos].first == ul_results[pos].first);
                REQUIRE(union_results[pos].second == Approx(ul_results[pos].second));
            }

            auto essential_counts = [&] {
                auto cursors = index.max_scored_cursors(term_ids, make_bm25(index));
                auto [non_essential, essential] =
                    maxscore_partition(gsl::make_span(cursors), threshold);
                return collect_payloads(v1::union_merge(
                    essential,
                    0,
                    [](auto count, [[maybe_unused]] auto&& cursor, [[maybe_unused]] auto idx) {
                        return count + 1;
                    }));
            }();
            REQUIRE(essential_counts.size() == inspect.documents);
            REQUIRE(ranges::accumulate(essential_counts, 0) == inspect.postings);
        }
    });
}
