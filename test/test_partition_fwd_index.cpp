#define CATCH_CONFIG_MAIN
#include "catch2/catch.hpp"

#include <algorithm>
#include <cstdio>
#include <iostream>
#include <string>

#include <boost/filesystem.hpp>
#include <fmt/ostream.h>
#include <gsl/span>
#include <range/v3/action/transform.hpp>
#include <range/v3/algorithm/stable_sort.hpp>
#include <range/v3/view/drop.hpp>
#include <range/v3/view/iota.hpp>
#include <range/v3/view/stride.hpp>
#include <range/v3/view/transform.hpp>

#include "binary_freq_collection.hpp"
#include "filesystem.hpp"
#include "forward_index_builder.hpp"
#include "invert.hpp"
#include "parser.hpp"
#include "payload_vector.hpp"
#include "pisa_config.hpp"
#include "sharding.hpp"
#include "temporary_directory.hpp"

using namespace boost::filesystem;
using namespace pisa;
using namespace pisa::literals;

using index_type = invert::Inverted_Index;

[[nodiscard]] auto next_plaintext_record(std::istream& in) -> std::optional<Document_Record>
{
    pisa::Plaintext_Record record;
    if (in >> record) {
        return std::make_optional<Document_Record>(
            std::move(record.trecid()), std::move(record.content()), std::move(record.url()));
    }
    return std::nullopt;
}

TEST_CASE("Expand shard", "[sharding]")
{
    REQUIRE(pisa::expand_shard("path", 17_s) == "path.017");
    REQUIRE(pisa::expand_shard("path.{}", 17_s) == "path.017");
    REQUIRE(pisa::expand_shard("path.{}.ext", 17_s) == "path.017.ext");
}

TEST_CASE("Resolve shards", "[sharding]")
{
    pisa::TemporaryDirectory dir;
    SECTION("No suffix")
    {
        for (auto f: std::vector<std::string>{"shard.000", "shard.001", "shard.002"}) {
            std::ofstream os((dir.path() / f).string());
            os << ".";
        }
        REQUIRE(
            pisa::resolve_shards((dir.path() / "shard.{}").string())
            == std::vector<Shard_Id>{Shard_Id(0), Shard_Id(1), Shard_Id(2)});
    }
    SECTION("With suffix")
    {
        for (auto f:
             std::vector<std::string>{"shard.000.docs", "shard.001.docs", "shard.002.docs"}) {
            std::ofstream os((dir.path() / f).string());
            os << ".";
        }
        REQUIRE(
            pisa::resolve_shards((dir.path() / "shard.{}").string(), ".docs")
            == std::vector<Shard_Id>{Shard_Id(0), Shard_Id(1), Shard_Id(2)});
    }
}

TEST_CASE("mapping_from_files", "[invert][unit]")
{
    std::istringstream full("D00\nD01\nD02\nD03\nD04\nD05\nD06\nD07\nD08\nD09\nD010\nD11");
    std::vector<std::unique_ptr<std::istream>> shards;
    shards.push_back(std::make_unique<std::istringstream>("D00\nD01\nD02"));
    shards.push_back(std::make_unique<std::istringstream>("D02\nD03\nD04"));
    shards.push_back(std::make_unique<std::istringstream>("D06\nD07\nD08\nD09\nD010\nD11"));
    auto stream_pointers =
        ranges::views::transform(shards, [](auto const& s) { return s.get(); }) | ranges::to_vector;
    REQUIRE(
        mapping_from_files(&full, gsl::span<std::istream*>(stream_pointers)).as_vector()
        == std::vector<Shard_Id>{0_s, 0_s, 0_s, 1_s, 1_s, 0_s, 2_s, 2_s, 2_s, 2_s, 2_s, 2_s});
}

TEST_CASE("create_random_mapping", "[invert][unit]")
{
    uint64_t seed = 88887;
    auto mapping = pisa::create_random_mapping(1000U, 13U, seed);
    VecMap<Shard_Id, int> counts(13, 0);
    VecMap<Document_Id> documents;
    for (auto&& [doc, shard]: mapping.entries()) {
        counts[shard] += 1;
        documents.push_back(doc);
    }
    std::sort(documents.begin(), documents.end());

    REQUIRE(
        documents.as_vector()
        == ranges::to_vector(ranges::views::iota(Document_Id{}, Document_Id{1000U})));
    REQUIRE(
        counts.as_vector() == std::vector<int>{77, 77, 77, 77, 77, 77, 77, 77, 77, 77, 77, 77, 76});
}

auto round_robin_mapping(int document_count, int shard_count)
{
    VecMap<Document_Id, Shard_Id> mapping(document_count);
    Shard_Id shard = 0_s;
    for (auto doc: ranges::views::iota(0_d, Document_Id{document_count})) {
        mapping[doc] = shard++;
        if (shard == Shard_Id{shard_count}) {
            shard = 0_s;
        }
    }
    return mapping;
}

void build_fwd_index(std::string const& output)
{
    std::string input(PISA_SOURCE_DIR "/test/test_data/clueweb1k.plaintext");
    std::ifstream is(input);
    pisa::Forward_Index_Builder builder;
    builder.build(
        is,
        output,
        next_plaintext_record,
        [] {
            return [](std::string&& term) -> std::string { return std::forward<std::string>(term); };
        },
        pisa::parse_plaintext_content,
        20'000,
        2);
}

template <typename Container>
auto shard_elements(Container const& container, Shard_Id shard_id, int shard_count)
{
    Container elems;
    for (auto const& val:
         ranges::views::drop(container, shard_id.as_int()) | ranges::views::stride(shard_count)) {
        elems.push_back(val);
    }
    return elems;
}

TEST_CASE("copy_sequence", "[invert][unit]")
{
    GIVEN("A test forward index")
    {
        pisa::TemporaryDirectory dir;
        std::string fwd_basename = (dir.path() / "fwd").string();
        std::string output = (dir.path() / "copy").string();
        int document_count = 1'000;
        build_fwd_index(fwd_basename);

        WHEN("All sequences are copied")
        {
            {
                std::ifstream is(fwd_basename);
                std::ofstream os(output);
                for ([[maybe_unused]] auto _: ranges::views::ints(0, document_count)) {
                    copy_sequence(is, os);
                }
            }

            THEN("Files are identical")
            {
                auto actual = io::load_data(output);
                auto expected = io::load_data(fwd_basename);
                expected.resize(actual.size());
                REQUIRE(actual == expected);
            }
        }
    }
}

TEST_CASE("Rearrange sequences", "[invert][integration]")
{
    GIVEN("A test forward index")
    {
        pisa::TemporaryDirectory dir;
        std::string fwd_basename = (dir.path() / "fwd").string();
        std::string output_basename = (dir.path() / "shards").string();
        int document_count = 1'000;
        build_fwd_index(fwd_basename);

        WHEN("Rearrange the sequences in a round-robin manner")
        {
            auto mapping = round_robin_mapping(document_count, 13);
            REQUIRE(mapping.size() == document_count);
            rearrange_sequences(fwd_basename, output_basename, mapping);
            auto shard_ids = ranges::views::iota(0_s, 13_s);

            THEN("Sequences are properly rearranged")
            {
                auto full = binary_collection(fwd_basename.c_str());
                auto full_iter = ++full.begin();
                std::vector<std::vector<std::uint32_t>> expected;
                std::transform(
                    full_iter, full.end(), std::back_inserter(expected), [](auto const& seq) {
                        return std::vector<std::uint32_t>(seq.begin(), seq.end());
                    });
                auto sorted_mapping = mapping.entries().collect();
                ranges::stable_sort(sorted_mapping, [](auto const& lhs, auto const& rhs) {
                    return std::make_pair(lhs.second, lhs.first)
                        < std::make_pair(rhs.second, rhs.first);
                });
                expected =
                    ranges::views::transform(
                        sorted_mapping, [&](auto&& entry) { return expected[entry.first.as_int()]; })
                    | ranges::to_vector;

                auto pos = expected.begin();
                for (auto shard: shard_ids) {
                    // std::vector<std::vector<std::uint32_t>> actual;
                    spdlog::info("Testing shard {}", shard.as_int());
                    spdlog::default_logger()->flush();
                    auto shard_coll = binary_collection(
                        fmt::format("{}.{:03d}", output_basename, shard.as_int()).c_str());
                    size_t doc = 0U;
                    CAPTURE(shard);
                    CAPTURE(doc);
                    for (auto iter = ++shard_coll.begin(); iter != shard_coll.end(); ++iter, ++pos) {
                        auto seq = *iter;
                        REQUIRE(*pos == std::vector<std::uint32_t>(seq.begin(), seq.end()));
                    }
                }
            }
        }
    }
}

TEST_CASE("partition_fwd_index", "[invert][integration]")
{
    GIVEN("A test forward index")
    {
        pisa::TemporaryDirectory dir;
        std::string fwd_basename = (dir.path() / "fwd").string();
        std::string output_basename = (dir.path() / "shards").string();
        int document_count = 1'000;
        build_fwd_index(fwd_basename);

        WHEN("Partition the forward index in a round-robin manner")
        {
            auto mapping = round_robin_mapping(document_count, 13);
            REQUIRE(mapping.size() == document_count);
            partition_fwd_index(fwd_basename, output_basename, mapping);
            auto shard_ids = ranges::views::iota(0_s, 13_s);

            THEN("Document titles are correctly partitioned")
            {
                auto original_titles =
                    io::read_string_vector(fmt::format("{}.documents", fwd_basename));
                for (auto shard_id: shard_ids) {
                    auto expected_titles = shard_elements(original_titles, shard_id, 13);
                    auto actual_titles = io::read_string_vector(
                        fmt::format("{}.{:03d}.documents", output_basename, shard_id.as_int()));
                    REQUIRE(actual_titles == expected_titles);
                }
            }
            AND_THEN("Document contents are identical wrt terms")
            {
                auto full = binary_collection(fwd_basename.c_str());
                auto full_iter = ++full.begin();
                auto full_terms = io::read_string_vector(fmt::format("{}.terms", fwd_basename));
                std::vector<binary_collection> shards;
                std::vector<typename binary_collection::const_iterator> shard_iterators;
                std::vector<std::vector<std::string>> shard_terms;
                for (auto shard: shard_ids) {
                    shards.push_back(binary_collection(
                        fmt::format("{}.{:03d}", output_basename, shard.as_int()).c_str()));
                    shard_terms.push_back(io::read_string_vector(
                        fmt::format("{}.{:03d}.terms", output_basename, shard.as_int()).c_str()));
                    shard_iterators.push_back(++shards.back().begin());
                }
                Shard_Id shard = 0_s;
                for (auto doc: ranges::views::iota(0_d, Document_Id{document_count})) {
                    CAPTURE(doc);
                    auto full_seq = *full_iter;
                    auto shard_seq = *shard_iterators[shard.as_int()];
                    std::vector<std::string> expected_documents(full_seq.size());
                    std::vector<std::string> actual_documents(shard_seq.size());
                    std::transform(
                        full_seq.begin(),
                        full_seq.end(),
                        expected_documents.begin(),
                        [&](auto const& id) { return full_terms[id]; });
                    std::transform(
                        shard_seq.begin(),
                        shard_seq.end(),
                        actual_documents.begin(),
                        [&](auto const& id) { return shard_terms[shard.as_int()][id]; });
                    REQUIRE(actual_documents == expected_documents);
                    ++full_iter;
                    ++shard_iterators[shard.as_int()];
                    shard += 1_s;
                    if (shard == 13_s) {
                        shard = 0_s;
                    }
                }
            }
            AND_THEN("Terms and term lexicon match")
            {
                for (auto shard: shard_ids) {
                    auto terms = io::read_string_vector(
                        fmt::format("{}.{:03d}.terms", output_basename, shard.as_int()).c_str());
                    mio::mmap_source m(
                        fmt::format("{}.{:03d}.termlex", output_basename, shard.as_int()).c_str());
                    auto lexicon = Payload_Vector<>::from(m);
                    REQUIRE(terms == std::vector<std::string>(lexicon.begin(), lexicon.end()));
                }
            }
            AND_THEN("Documents and document lexicon match")
            {
                for (auto shard: shard_ids) {
                    auto documents = io::read_string_vector(
                        fmt::format("{}.{:03d}.documents", output_basename, shard.as_int()).c_str());
                    mio::mmap_source m(
                        fmt::format("{}.{:03d}.doclex", output_basename, shard.as_int()).c_str());
                    auto lexicon = Payload_Vector<>::from(m);
                    REQUIRE(documents == std::vector<std::string>(lexicon.begin(), lexicon.end()));
                }
            }
        }
    }
}
