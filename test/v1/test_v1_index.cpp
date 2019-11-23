#define CATCH_CONFIG_MAIN
#include "catch2/catch.hpp"

#include <string>

#include <boost/iostreams/device/back_inserter.hpp>
#include <boost/iostreams/stream.hpp>
#include <tbb/task_scheduler_init.h>

#include "../temporary_directory.hpp"
#include "binary_collection.hpp"
#include "codec/simdbp.hpp"
#include "pisa_config.hpp"
#include "v1/blocked_cursor.hpp"
#include "v1/cursor/collect.hpp"
#include "v1/index.hpp"
#include "v1/index_builder.hpp"
#include "v1/index_metadata.hpp"
#include "v1/types.hpp"

using pisa::binary_freq_collection;
using pisa::v1::BlockedReader;
using pisa::v1::BlockedWriter;
using pisa::v1::compress_binary_collection;
using pisa::v1::DocId;
using pisa::v1::Frequency;
using pisa::v1::index_runner;
using pisa::v1::IndexMetadata;
using pisa::v1::RawReader;
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
    auto run = index_runner(meta,
                            RawReader<DocId>{},
                            BlockedReader<pisa::simdbp_block, false>{},
                            BlockedReader<pisa::simdbp_block, true>{});
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
                               make_writer(BlockedWriter<::pisa::simdbp_block, true>{}),
                               make_writer(BlockedWriter<::pisa::simdbp_block, false>{}));
    auto meta = IndexMetadata::from_file((tmpdir.path() / "index.yml").string());
    REQUIRE(meta.documents.postings == (tmpdir.path() / "index.documents").string());
    REQUIRE(meta.documents.offsets == (tmpdir.path() / "index.document_offsets").string());
    REQUIRE(meta.frequencies.postings == (tmpdir.path() / "index.frequencies").string());
    REQUIRE(meta.frequencies.offsets == (tmpdir.path() / "index.frequency_offsets").string());
    REQUIRE(meta.document_lengths_path == (tmpdir.path() / "index.document_lengths").string());
    auto run = index_runner(meta,
                            RawReader<DocId>{},
                            BlockedReader<pisa::simdbp_block, false>{},
                            BlockedReader<pisa::simdbp_block, true>{});
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
