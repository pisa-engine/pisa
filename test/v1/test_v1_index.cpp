#define CATCH_CONFIG_MAIN
#include "catch2/catch.hpp"

#include <string>

#include <boost/iostreams/device/back_inserter.hpp>
#include <boost/iostreams/stream.hpp>
#include <tbb/task_scheduler_init.h>

#include "../temporary_directory.hpp"
#include "binary_collection.hpp"
#include "codec/simdbp.hpp"
#include "index_fixture.hpp"
#include "pisa_config.hpp"
#include "v1/bit_sequence_cursor.hpp"
#include "v1/blocked_cursor.hpp"
#include "v1/cursor/collect.hpp"
#include "v1/cursor/for_each.hpp"
#include "v1/default_index_runner.hpp"
#include "v1/index.hpp"
#include "v1/index_builder.hpp"
#include "v1/index_metadata.hpp"
#include "v1/raw_cursor.hpp"
#include "v1/scorer/bm25.hpp"
#include "v1/sequence/partitioned_sequence.hpp"
#include "v1/sequence/positive_sequence.hpp"
#include "v1/types.hpp"

using pisa::binary_freq_collection;
using pisa::v1::BigramMetadata;
using pisa::v1::build_pair_index;
using pisa::v1::compress_binary_collection;
using pisa::v1::DocId;
using pisa::v1::DocumentBitSequenceCursor;
using pisa::v1::DocumentBlockedCursor;
using pisa::v1::DocumentBlockedWriter;
using pisa::v1::for_each;
using pisa::v1::Frequency;
using pisa::v1::index_runner;
using pisa::v1::IndexMetadata;
using pisa::v1::make_bm25;
using pisa::v1::PartitionedSequence;
using pisa::v1::PayloadBitSequenceCursor;
using pisa::v1::PayloadBlockedCursor;
using pisa::v1::PayloadBlockedWriter;
using pisa::v1::PositiveSequence;
using pisa::v1::PostingFilePaths;
using pisa::v1::Query;
using pisa::v1::RawCursor;
using pisa::v1::RawWriter;
using pisa::v1::TermId;

TEST_CASE("Binary collection index", "[v1][unit]")
{
    tbb::task_scheduler_init init(8);
    Temporary_Directory tmpdir;
    auto bci = binary_freq_collection(PISA_SOURCE_DIR "/test/test_data/test_collection");
    compress_binary_collection(PISA_SOURCE_DIR "/test/test_data/test_collection",
                               (tmpdir.path() / "fwd").string(),
                               (tmpdir.path() / "index").string(),
                               8,
                               make_writer(RawWriter<std::uint32_t>{}),
                               make_writer(RawWriter<std::uint32_t>{}));
    auto meta = IndexMetadata::from_file((tmpdir.path() / "index.yml").string());
    REQUIRE(meta.documents.postings == (tmpdir.path() / "index.documents").string());
    REQUIRE(meta.documents.offsets == (tmpdir.path() / "index.document_offsets").string());
    REQUIRE(meta.frequencies.postings == (tmpdir.path() / "index.frequencies").string());
    REQUIRE(meta.frequencies.offsets == (tmpdir.path() / "index.frequency_offsets").string());
    REQUIRE(meta.document_lengths_path == (tmpdir.path() / "index.document_lengths").string());
    auto run = index_runner(meta);
    run([&](auto index) {
        REQUIRE(bci.num_docs() == index.num_documents());
        REQUIRE(bci.size() == index.num_terms());
        auto bci_iter = bci.begin();
        for (auto term = 0; term < 1'000; term += 1) {
            REQUIRE(std::vector<std::uint32_t>(bci_iter->docs.begin(), bci_iter->docs.end())
                    == collect(index.documents(term)));
            REQUIRE(std::vector<std::uint32_t>(bci_iter->freqs.begin(), bci_iter->freqs.end())
                    == collect(index.payloads(term)));
            ++bci_iter;
        }
    });
}

TEST_CASE("Binary collection index -- SIMDBP", "[v1][unit]")
{
    tbb::task_scheduler_init init(8);
    Temporary_Directory tmpdir;
    auto bci = binary_freq_collection(PISA_SOURCE_DIR "/test/test_data/test_collection");
    compress_binary_collection(PISA_SOURCE_DIR "/test/test_data/test_collection",
                               (tmpdir.path() / "fwd").string(),
                               (tmpdir.path() / "index").string(),
                               8,
                               make_writer(DocumentBlockedWriter<::pisa::simdbp_block>{}),
                               make_writer(PayloadBlockedWriter<::pisa::simdbp_block>{}));
    auto meta = IndexMetadata::from_file((tmpdir.path() / "index.yml").string());
    REQUIRE(meta.documents.postings == (tmpdir.path() / "index.documents").string());
    REQUIRE(meta.documents.offsets == (tmpdir.path() / "index.document_offsets").string());
    REQUIRE(meta.frequencies.postings == (tmpdir.path() / "index.frequencies").string());
    REQUIRE(meta.frequencies.offsets == (tmpdir.path() / "index.frequency_offsets").string());
    REQUIRE(meta.document_lengths_path == (tmpdir.path() / "index.document_lengths").string());
    auto run = index_runner(meta);
    run([&](auto index) {
        REQUIRE(bci.num_docs() == index.num_documents());
        REQUIRE(bci.size() == index.num_terms());
        auto bci_iter = bci.begin();
        for (auto term = 0; term < 1'000; term += 1) {
            REQUIRE(std::vector<std::uint32_t>(bci_iter->docs.begin(), bci_iter->docs.end())
                    == collect(index.documents(term)));
            REQUIRE(std::vector<std::uint32_t>(bci_iter->freqs.begin(), bci_iter->freqs.end())
                    == collect(index.payloads(term)));
            ++bci_iter;
        }
    });
}

TEMPLATE_TEST_CASE("Index",
                   "[v1][integration]",
                   (IndexFixture<RawCursor<DocId>, RawCursor<Frequency>, RawCursor<std::uint8_t>>),
                   (IndexFixture<DocumentBlockedCursor<::pisa::simdbp_block>,
                                 PayloadBlockedCursor<::pisa::simdbp_block>,
                                 RawCursor<std::uint8_t>>),
                   (IndexFixture<DocumentBitSequenceCursor<pisa::v1::IndexedSequence>,
                                 PayloadBitSequenceCursor<pisa::v1::PositiveSequence<>>,
                                 RawCursor<std::uint8_t>>),
                   (IndexFixture<DocumentBitSequenceCursor<pisa::v1::PartitionedSequence<>>,
                                 PayloadBitSequenceCursor<pisa::v1::PositiveSequence<>>,
                                 RawCursor<std::uint8_t>>))
{
    tbb::task_scheduler_init init{1};
    TestType fixture(false, false, false, false);
    auto index_basename = (fixture.tmpdir().path() / "inv").string();
    auto meta = IndexMetadata::from_file(fmt::format("{}.yml", index_basename));
    auto run = pisa::v1::index_runner(meta,
                                      std::make_tuple(fixture.document_reader()),
                                      std::make_tuple(fixture.frequency_reader()));
    auto bci = binary_freq_collection(PISA_SOURCE_DIR "/test/test_data/test_collection");
    run([&](auto&& index) {
        REQUIRE(bci.num_docs() == index.num_documents());
        REQUIRE(bci.size() == index.num_terms());
        auto bci_iter = bci.begin();
        for (auto term = 0; term < 1'000; term += 1) {
            REQUIRE(std::vector<std::uint32_t>(bci_iter->docs.begin(), bci_iter->docs.end())
                    == collect(index.documents(term)));
            REQUIRE(std::vector<std::uint32_t>(bci_iter->freqs.begin(), bci_iter->freqs.end())
                    == collect(index.payloads(term)));
            ++bci_iter;
        }
    });
}

TEST_CASE("Select best bigrams", "[v1][integration]")
{
    tbb::task_scheduler_init init;
    IndexFixture<RawCursor<DocId>, RawCursor<Frequency>, RawCursor<std::uint8_t>> fixture(
        false, false, false, false);
    std::string index_basename = (fixture.tmpdir().path() / "inv").string();
    auto meta = IndexMetadata::from_file(fmt::format("{}.yml", index_basename));
    {
        std::vector<Query> queries;
        auto best_bigrams = select_best_bigrams(meta, queries, 10);
        REQUIRE(best_bigrams.empty());
    }
    {
        std::vector<Query> queries = {Query::from_ids(0, 1).with_probability(0.1)};
        auto best_bigrams = select_best_bigrams(meta, queries, 10);
        REQUIRE(best_bigrams == std::vector<std::pair<TermId, TermId>>{{0, 1}});
    }
    {
        std::vector<Query> queries = {
            Query::from_ids(0, 1).with_probability(0.2), // u: 3758, i: 808  u/i: 4.650990099009901
            Query::from_ids(1, 2).with_probability(0.2), // u: 3961, i: 734  u/i: 5.3964577656675745
            Query::from_ids(2, 3).with_probability(0.2), // u: 2298, i: 61   u/i: 37.67213114754098
            Query::from_ids(3, 4).with_probability(0.2), // u: 90,   i: 3    u/i: 30.0
            Query::from_ids(4, 5).with_probability(0.2), // u: 21,   i: 1    u/i: 21.0
            Query::from_ids(5, 6).with_probability(0.2), // u: 8,    i: 3    u/i: 2.6666666666666665
            Query::from_ids(6, 7).with_probability(0.2), // u: 4,    i: 0    u/i: ---
            Query::from_ids(7, 8).with_probability(0.2), // u: 2,    i: 1    u/i: 2
            Query::from_ids(8, 9).with_probability(0.2), // u: 2,    i: 1    u/i: 2
            Query::from_ids(9, 10).with_probability(0.2) // u: 2,    i: 1    u/i: 2
        };
        auto best_bigrams = select_best_bigrams(meta, queries, 3);
        REQUIRE(best_bigrams == std::vector<std::pair<TermId, TermId>>{{2, 3}, {3, 4}, {4, 5}});
    }
    {
        std::vector<Query> queries = {
            Query::from_ids(0, 1).with_probability(0.2), // u: 3758, i: 808  u/i: 4.650990099009901
            Query::from_ids(1, 2).with_probability(0.2), // u: 3961, i: 734  u/i: 5.3964577656675745
            Query::from_ids(2, 3).with_probability(0.2), // u: 2298, i: 61   u/i: 37.67213114754098
            Query::from_ids(3, 4).with_probability(0.4), // u: 90,   i: 3    u/i: 30.0
            Query::from_ids(4, 5).with_probability(0.01), // u: 21,   i: 1    u/i: 21.0
            Query::from_ids(5, 6).with_probability(0.2), // u: 8,    i: 3    u/i: 2.6666666666666665
            Query::from_ids(6, 7).with_probability(0.2), // u: 4,    i: 0    u/i: ---
            Query::from_ids(7, 8).with_probability(0.2), // u: 2,    i: 1    u/i: 2
            Query::from_ids(8, 9).with_probability(0.2), // u: 2,    i: 1    u/i: 2
            Query::from_ids(9, 10).with_probability(0.2) // u: 2,    i: 1    u/i: 2
        };
        auto best_bigrams = select_best_bigrams(meta, queries, 3);
        REQUIRE(best_bigrams == std::vector<std::pair<TermId, TermId>>{{3, 4}, {2, 3}, {1, 2}});
    }
}

TEST_CASE("Build pair index", "[v1][integration]")
{
    tbb::task_scheduler_init init;
    IndexFixture<RawCursor<DocId>, RawCursor<Frequency>, RawCursor<std::uint8_t>> fixture(
        true, true, true, false);
    std::string index_basename = (fixture.tmpdir().path() / "inv").string();
    auto meta = IndexMetadata::from_file(fmt::format("{}.yml", index_basename));
    SECTION("In place")
    {
        build_pair_index(meta, {{0, 1}, {0, 2}, {0, 3}, {0, 4}, {0, 5}}, tl::nullopt, 4);
        auto run = index_runner(IndexMetadata::from_file(fmt::format("{}.yml", index_basename)));
        run([&](auto&& index) {
            REQUIRE(index.bigram_cursor(0, 1).has_value());
            REQUIRE(index.bigram_cursor(1, 0).has_value());
            REQUIRE(not index.bigram_cursor(1, 2).has_value());
            REQUIRE(not index.bigram_cursor(2, 1).has_value());
        });
    }
    SECTION("Cloned")
    {
        auto cloned_basename = (fixture.tmpdir().path() / "cloned").string();
        build_pair_index(
            meta, {{0, 1}, {0, 2}, {0, 3}, {0, 4}, {0, 5}}, tl::make_optional(cloned_basename), 4);
        SECTION("Original index has no pairs")
        {
            auto run =
                index_runner(IndexMetadata::from_file(fmt::format("{}.yml", index_basename)));
            run([&](auto&& index) {
                REQUIRE_THROWS_AS(index.bigram_cursor(0, 1), std::logic_error);
            });
        }
        SECTION("New index has pairs")
        {
            auto run =
                index_runner(IndexMetadata::from_file(fmt::format("{}.yml", cloned_basename)));
            run([&](auto&& index) {
                REQUIRE(index.bigram_cursor(0, 1).has_value());
                REQUIRE(index.bigram_cursor(1, 0).has_value());
                REQUIRE(not index.bigram_cursor(1, 2).has_value());
                REQUIRE(not index.bigram_cursor(2, 1).has_value());
            });
        }
    }
}
