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
#include "v1/default_index_runner.hpp"
#include "v1/index.hpp"
#include "v1/index_builder.hpp"
#include "v1/index_metadata.hpp"
#include "v1/raw_cursor.hpp"
#include "v1/sequence/partitioned_sequence.hpp"
#include "v1/sequence/positive_sequence.hpp"
#include "v1/types.hpp"

using pisa::binary_freq_collection;
using pisa::v1::compress_binary_collection;
using pisa::v1::DocId;
using pisa::v1::DocumentBitSequenceCursor;
using pisa::v1::DocumentBlockedCursor;
using pisa::v1::DocumentBlockedWriter;
using pisa::v1::Frequency;
using pisa::v1::index_runner;
using pisa::v1::IndexMetadata;
using pisa::v1::PartitionedSequence;
using pisa::v1::PayloadBitSequenceCursor;
using pisa::v1::PayloadBlockedCursor;
using pisa::v1::PayloadBlockedWriter;
using pisa::v1::PositiveSequence;
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
