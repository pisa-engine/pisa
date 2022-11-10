#include <taily.hpp>
#define CATCH_CONFIG_MAIN
#include "catch2/catch.hpp"

#include <array>

#include <boost/interprocess/streams/bufferstream.hpp>
#include <gsl/span>

#include "binary_freq_collection.hpp"
#include "filesystem.hpp"
#include "io.hpp"
#include "memory_source.hpp"
#include "pisa_config.hpp"
#include "scorer/scorer.hpp"
#include "taily_stats.hpp"
#include "temporary_directory.hpp"
#include "wand_data.hpp"
#include "wand_data_raw.hpp"

using taily::Feature_Statistics;

void write_documents(boost::filesystem::path const& path)
{
    pisa::io::write_data(
        path.string(),
        gsl::span<std::byte const>(std::array<std::byte, 44>{
            std::byte{1}, std::byte{0}, std::byte{0}, std::byte{0},
            std::byte{6}, std::byte{0}, std::byte{0}, std::byte{0},  //< #docs
            std::byte{2}, std::byte{0}, std::byte{0}, std::byte{0},  //< term 0
            std::byte{0}, std::byte{0}, std::byte{0}, std::byte{0},  //< term 0
            std::byte{2}, std::byte{0}, std::byte{0}, std::byte{0},  //< term 0
            std::byte{3}, std::byte{0}, std::byte{0}, std::byte{0},  //< term 1
            std::byte{1}, std::byte{0}, std::byte{0}, std::byte{0},  //< term 1
            std::byte{3}, std::byte{0}, std::byte{0}, std::byte{0},  //< term 1
            std::byte{4}, std::byte{0}, std::byte{0}, std::byte{0},  //< term 1
            std::byte{1}, std::byte{0}, std::byte{0}, std::byte{0},  //< term 2
            std::byte{5}, std::byte{0}, std::byte{0}, std::byte{0},  //< term 2
        }));
}

void write_frequencies(boost::filesystem::path const& path)
{
    pisa::io::write_data(
        path.string(),
        gsl::span<std::byte const>(std::array<std::byte, 36>{
            std::byte{2}, std::byte{0}, std::byte{0}, std::byte{0},  //< term 0
            std::byte{1}, std::byte{0}, std::byte{0}, std::byte{0},  //< term 0
            std::byte{1}, std::byte{0}, std::byte{0}, std::byte{0},  //< term 0
            std::byte{3}, std::byte{0}, std::byte{0}, std::byte{0},  //< term 1
            std::byte{2}, std::byte{0}, std::byte{0}, std::byte{0},  //< term 1
            std::byte{1}, std::byte{0}, std::byte{0}, std::byte{0},  //< term 1
            std::byte{5}, std::byte{0}, std::byte{0}, std::byte{0},  //< term 1
            std::byte{1}, std::byte{0}, std::byte{0}, std::byte{0},  //< term 2
            std::byte{4}, std::byte{0}, std::byte{0}, std::byte{0},  //< term 2
        }));
}

void write_sizes(boost::filesystem::path const& path)
{
    pisa::io::write_data(
        path.string(),
        gsl::span<std::byte const>(std::array<std::byte, 28>{
            std::byte{5}, std::byte{0}, std::byte{0}, std::byte{0},  //
            std::byte{1}, std::byte{0}, std::byte{0}, std::byte{0},  //
            std::byte{1}, std::byte{0}, std::byte{0}, std::byte{0},  //
            std::byte{1}, std::byte{0}, std::byte{0}, std::byte{0},  //
            std::byte{1}, std::byte{0}, std::byte{0}, std::byte{0},  //
            std::byte{1}, std::byte{0}, std::byte{0}, std::byte{0},  //
            std::byte{1}, std::byte{0}, std::byte{0}, std::byte{0},  //
        }));
}

TEST_CASE("Extract Taily feature stats", "[taily][unit]")
{
    GIVEN("Collection")
    {
        pisa::TemporaryDirectory tmpdir;
        write_documents(tmpdir.path() / "coll.docs");
        write_frequencies(tmpdir.path() / "coll.freqs");
        write_sizes(tmpdir.path() / "coll.sizes");

        auto wand_data_path = tmpdir.path() / "wdata";
        auto collection_path = tmpdir.path() / "coll";
        create_wand_data(
            wand_data_path.string(),
            collection_path.string(),
            pisa::FixedBlock{128},
            ScorerParams("quantized"),
            false,
            false,
            true,
            {});

        pisa::binary_freq_collection collection(collection_path.c_str());
        pisa::wand_data<pisa::wand_data_raw> wdata(pisa::MemorySource::mapped_file(wand_data_path));

        WHEN("Extract feature stats")
        {
            auto stats = pisa::extract_feature_stats(
                collection, pisa::scorer::from_params(ScorerParams("quantized"), wdata));

            THEN("Correct stats")
            {
                REQUIRE(stats.size() == 3);

                REQUIRE(stats[0].frequency == 2);
                REQUIRE(stats[0].expected_value == Approx(1));
                REQUIRE(stats[0].variance == Approx(0.0));

                REQUIRE(stats[1].frequency == 3);
                REQUIRE(stats[1].expected_value == Approx(8.0 / 3.0));
                REQUIRE(stats[1].variance == Approx(2.8888888889));

                REQUIRE(stats[2].frequency == 1);
                REQUIRE(stats[2].expected_value == Approx(4.0));
                REQUIRE(stats[2].variance == Approx(0.0));
            }
        }
    }
}

TEST_CASE("Write Taily feature stats", "[taily][unit]")
{
    pisa::TemporaryDirectory tmpdir;
    auto stats_path = tmpdir.path() / "taily";
    GIVEN("Feature statistics")
    {
        std::vector<Feature_Statistics> stats{Feature_Statistics{1.0, 2.0, 10},
                                              Feature_Statistics{3.0, 4.0, 20},
                                              Feature_Statistics{5.0, 6.0, 30}};

        WHEN("Stats written to a file")
        {
            pisa::write_feature_stats(stats, 10, stats_path.string());

            THEN("Stats can be read back")
            {
                auto stats = pisa::TailyStats::from_mapped(stats_path.string());
                REQUIRE(stats.num_documents() == 10);
                REQUIRE(stats.num_terms() == 3);

                REQUIRE(stats.term_stats(0).expected_value == Approx(1.0));
                REQUIRE(stats.term_stats(0).variance == Approx(2.0));
                REQUIRE(stats.term_stats(0).frequency == Approx(10));

                REQUIRE(stats.term_stats(1).expected_value == Approx(3.0));
                REQUIRE(stats.term_stats(1).variance == Approx(4.0));
                REQUIRE(stats.term_stats(1).frequency == Approx(20));

                REQUIRE(stats.term_stats(2).expected_value == Approx(5.0));
                REQUIRE(stats.term_stats(2).variance == Approx(6.0));
                REQUIRE(stats.term_stats(2).frequency == Approx(30));

                REQUIRE_THROWS_AS(stats.term_stats(3), std::out_of_range);

                auto query_stats = stats.query_stats(pisa::Query{{}, {0, 1, 2}, {}});

                REQUIRE(query_stats.collection_size == 10);

                REQUIRE(query_stats.term_stats[0].expected_value == Approx(1.0));
                REQUIRE(query_stats.term_stats[0].variance == Approx(2.0));
                REQUIRE(query_stats.term_stats[0].frequency == Approx(10));

                REQUIRE(query_stats.term_stats[1].expected_value == Approx(3.0));
                REQUIRE(query_stats.term_stats[1].variance == Approx(4.0));
                REQUIRE(query_stats.term_stats[1].frequency == Approx(20));

                REQUIRE(query_stats.term_stats[2].expected_value == Approx(5.0));
                REQUIRE(query_stats.term_stats[2].variance == Approx(6.0));
                REQUIRE(query_stats.term_stats[2].frequency == Approx(30));
            }
        }
    }
}
