#define CATCH_CONFIG_MAIN

#include <catch2/catch.hpp>

#include "pisa/cursor/block_max_scored_cursor.hpp"
#include "pisa/cursor/cursor.hpp"
#include "pisa/cursor/max_scored_cursor.hpp"
#include "pisa/cursor/scored_cursor.hpp"
#include "pisa/scorer/quantized.hpp"

#include "in_memory_index.hpp"

using namespace pisa;

TEST_CASE("TODO")
{
    InMemoryIndex index{{
                            {0},  // 0
                            {0, 1, 2},  // 1
                            {0},  // 2
                            {0},  // 3
                            {0},  // 4
                            {0, 1, 4},  // 5
                            {1, 4, 8},  // 6
                        },
                        {
                            {1},  // 0
                            {1, 1, 1},  // 1
                            {1},  // 2
                            {1},  // 3
                            {1},  // 4
                            {1, 1, 1},  // 5
                            {1, 1, 1},  // 6
                        },
                        10};
    InMemoryWand wand{{1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0}, 10};
    quantized<InMemoryWand> scorer(wand);
    Query query{"Q1", {0, 1, 1, 2}, {}};

    auto collect_scores = [&index](auto&& cursor) {
        std::vector<float> scores;
        while (cursor.docid() < index.size()) {
            scores.push_back(cursor.score());
            cursor.next();
        }
        return scores;
    };

    SECTION("Scored cursor")
    {
        WHEN("No weights are requested")
        {
            std::vector<std::vector<float>> scores;
            auto cursors = make_scored_cursors(index, scorer, query);
            std::transform(
                cursors.begin(), cursors.end(), std::back_inserter(scores), collect_scores);
            CHECK(scores == std::vector<std::vector<float>>{{1}, {1, 1, 1}, {1}});
        }

        WHEN("Weights are requested")
        {
            std::vector<std::vector<float>> scores;
            auto cursors = make_scored_cursors(index, scorer, query, true);
            std::transform(
                cursors.begin(), cursors.end(), std::back_inserter(scores), collect_scores);
            CHECK(scores == std::vector<std::vector<float>>{{1}, {2, 2, 2}, {1}});
        }
    }

    SECTION("Max-scored cursor")
    {
        WHEN("No weights are requested")
        {
            std::vector<std::vector<float>> scores;
            auto cursors = make_max_scored_cursors(index, wand, scorer, query);
            CHECK(cursors[0].max_score() == 1.0);
            CHECK(cursors[1].max_score() == 1.0);
            CHECK(cursors[2].max_score() == 1.0);
            std::transform(
                cursors.begin(), cursors.end(), std::back_inserter(scores), collect_scores);
            CHECK(scores == std::vector<std::vector<float>>{{1}, {1, 1, 1}, {1}});
        }

        WHEN("Weights are requested")
        {
            std::vector<std::vector<float>> scores;
            auto cursors = make_max_scored_cursors(index, wand, scorer, query, true);
            CHECK(cursors[0].max_score() == 1.0);
            CHECK(cursors[1].max_score() == 2.0);
            CHECK(cursors[2].max_score() == 1.0);
            std::transform(
                cursors.begin(), cursors.end(), std::back_inserter(scores), collect_scores);
            CHECK(scores == std::vector<std::vector<float>>{{1}, {2, 2, 2}, {1}});
        }
    }
}
