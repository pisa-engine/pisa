#define CATCH_CONFIG_MAIN
#include "catch2/catch.hpp"

#include <string>

#include <boost/iostreams/device/back_inserter.hpp>
#include <boost/iostreams/stream.hpp>
#include <tbb/task_scheduler_init.h>

#include "pisa_config.hpp"
#include "temporary_directory.hpp"
#include "v1/cursor/collect.hpp"
#include "v1/index.hpp"
#include "v1/index_builder.hpp"
#include "v1/index_metadata.hpp"
#include "v1/types.hpp"

using pisa::v1::binary_collection_index;
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
    auto bci = binary_collection_index(PISA_SOURCE_DIR "/test/test_data/test_collection");
    compress_binary_collection(PISA_SOURCE_DIR "/test/test_data/test_collection",
                               (tmpdir.path() / "index").string(),
                               8,
                               make_writer(RawWriter<DocId>{}),
                               make_writer(RawWriter<Frequency>{}));
    auto meta = IndexMetadata::from_file((tmpdir.path() / "index.ini").string());
    REQUIRE(meta.documents.postings == (tmpdir.path() / "index.documents").string());
    REQUIRE(meta.documents.offsets == (tmpdir.path() / "index.document_offsets").string());
    REQUIRE(meta.frequencies.postings == (tmpdir.path() / "index.frequencies").string());
    REQUIRE(meta.frequencies.offsets == (tmpdir.path() / "index.frequency_offsets").string());
    REQUIRE(meta.document_lengths_path == (tmpdir.path() / "index.document_lengths").string());
    auto run = index_runner(meta, RawReader<DocId>{}, RawReader<Frequency>{});
    run([&](auto index) {
        REQUIRE(bci.avg_document_length() == index.avg_document_length());
        REQUIRE(bci.num_documents() == index.num_documents());
        REQUIRE(bci.num_terms() == index.num_terms());
        for (auto term = 0; term < bci.num_terms(); term += 1) {
            REQUIRE(collect(bci.documents(term)) == collect(index.documents(term)));
            REQUIRE(collect(bci.payloads(term)) == collect(index.payloads(term)));
        }
    });
}
