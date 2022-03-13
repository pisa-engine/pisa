#define CATCH_CONFIG_MAIN
#include <catch2/catch.hpp>

#include <algorithm>

#include <rapidcheck.h>

#include "pisa/topk_queue.hpp"

using namespace rc;

/// Scale scores to (0, 1] to get smaller score differences.
auto scale_unit(float score) -> float
{
    return std::max(score / std::numeric_limits<float>::max(), std::numeric_limits<float>::min());
}

auto gen_postings(int min_length, int max_length)
{
    return gen::mapcat(gen::inRange(min_length, max_length), [](int length) {
        return rc::gen::pair(
            gen::container<std::vector<float>>(length, gen::map(gen::positive<float>(), scale_unit)),
            gen::unique<std::vector<std::uint32_t>>(length, gen::positive<std::uint32_t>()));
    });
}

auto gen_quantized_postings(int min_length, int max_length)
{
    return gen::mapcat(gen::inRange(min_length, max_length), [](int length) {
        return rc::gen::pair(
            gen::container<std::vector<float>>(
                length,
                gen::map(gen::positive<std::uint8_t>(), [](auto i) { return static_cast<float>(i); })),
            gen::unique<std::vector<std::uint32_t>>(length, gen::positive<std::uint32_t>()));
    });
}

void accumulate(
    pisa::topk_queue& topk, std::vector<float> const& scores, std::vector<std::uint32_t> const& docids)
{
    for (int posting = 0; posting < docids.size(); ++posting) {
        topk.insert(scores[posting], docids[posting]);
    }
}

auto kth(std::vector<float> scores, int k) -> float
{
    std::sort(scores.begin(), scores.end(), std::greater{});
    return scores.at(k - 1);
}

TEST_CASE("Threshold", "[topk_queue][prop]")
{
    SECTION("When initial = 0.0, the final threshold is the k-th score")
    {
        check([] {
            auto [scores, docids] = *gen_postings(10, 1000);

            pisa::topk_queue topk(10);
            accumulate(topk, scores, docids);

            auto expected = kth(scores, 10);
            REQUIRE(topk.true_threshold() == expected);
            REQUIRE(topk.effective_threshold() == expected);
            REQUIRE(topk.initial_threshold() == 0.0);
        });
    }

    SECTION("When too few postings, then final threshold 0.0")
    {
        check([] {
            auto [scores, docids] = *gen_postings(1, 9);
            pisa::topk_queue topk(10);
            accumulate(topk, scores, docids);
            REQUIRE(topk.true_threshold() == 0.0);
            REQUIRE(topk.effective_threshold() == 0.0);
            REQUIRE(topk.initial_threshold() == 0.0);
        });
    }

    SECTION("When too few postings and initial threshold, then final threshold equal to initial")
    {
        check([] {
            auto [scores, docids] = *gen_postings(1, 9);
            auto initial = *gen::positive<float>();
            pisa::topk_queue topk(10, initial);
            accumulate(topk, scores, docids);
            REQUIRE(topk.true_threshold() == 0.0);
            REQUIRE(topk.effective_threshold() < topk.initial_threshold());
            REQUIRE(topk.initial_threshold() == initial);
        });
    }

    SECTION("When initial is exact, final is the same")
    {
        SECTION("floats")
        {
            check([] {
                auto [scores, docids] = *gen_postings(10, 1000);
                auto initial = kth(scores, 10);
                pisa::topk_queue topk(10, initial);
                accumulate(topk, scores, docids);
                REQUIRE(topk.initial_threshold() == initial);
                REQUIRE(topk.true_threshold() == topk.initial_threshold());
                REQUIRE(topk.effective_threshold() == topk.initial_threshold());
            });
        }
        SECTION("quantized")
        {
            check([] {
                auto [scores, docids] = *gen_quantized_postings(10, 1000);
                auto initial = kth(scores, 10);
                pisa::topk_queue topk(10, initial);
                accumulate(topk, scores, docids);
                REQUIRE(topk.initial_threshold() == initial);
                REQUIRE(topk.true_threshold() == topk.initial_threshold());
                REQUIRE(topk.effective_threshold() == topk.initial_threshold());
            });
        }
    }

    SECTION("When initial is too high, true is lower than effective")
    {
        check([] {
            auto [scores, docids] = *gen_postings(10, 1000);
            auto initial = std::nextafter(kth(scores, 10), std::numeric_limits<float>::max());
            pisa::topk_queue topk(10, initial);
            accumulate(topk, scores, docids);
            CAPTURE(topk.topk());
            REQUIRE(topk.initial_threshold() == initial);
            REQUIRE(topk.true_threshold() < topk.effective_threshold());
        });
    }

    SECTION("Threshold never decreases")
    {
        check([] {
            auto [scores, docids] = *gen_postings(10, 1000);

            // Pick a document to use as threshold
            auto n = *gen::inRange<std::size_t>(0, docids.size());
            auto initial = scores[n];
            pisa::topk_queue topk(10, initial);

            std::vector<float> thresholds;
            std::vector<float> true_thresholds;
            for (int posting = 0; posting < docids.size(); ++posting) {
                topk.insert(scores[posting], docids[posting]);
                thresholds.push_back(topk.effective_threshold());
                true_thresholds.push_back(topk.true_threshold());
            }

            CAPTURE(thresholds);
            REQUIRE(std::is_sorted(thresholds.begin(), thresholds.end()));
            REQUIRE(std::is_sorted(true_thresholds.begin(), true_thresholds.end()));
        });
    }
}
