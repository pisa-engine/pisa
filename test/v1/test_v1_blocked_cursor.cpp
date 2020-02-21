#define CATCH_CONFIG_MAIN
#include "catch2/catch.hpp"

#include <string>

#include <boost/iostreams/device/back_inserter.hpp>
#include <boost/iostreams/stream.hpp>
#include <tbb/task_scheduler_init.h>

#include "../temporary_directory.hpp"
#include "codec/simdbp.hpp"
#include "pisa_config.hpp"
#include "v1/blocked_cursor.hpp"
#include "v1/cursor/collect.hpp"
#include "v1/index.hpp"
#include "v1/index_builder.hpp"
#include "v1/index_metadata.hpp"
#include "v1/io.hpp"
#include "v1/posting_builder.hpp"
#include "v1/types.hpp"

using pisa::v1::collect;
using pisa::v1::DocId;
using pisa::v1::DocumentBlockedReader;
using pisa::v1::DocumentBlockedWriter;
using pisa::v1::Frequency;
using pisa::v1::IndexRunner;
using pisa::v1::PayloadBlockedReader;
using pisa::v1::PayloadBlockedWriter;
using pisa::v1::PostingBuilder;
using pisa::v1::PostingData;
using pisa::v1::RawReader;
using pisa::v1::read_sizes;
using pisa::v1::TermId;

TEST_CASE("Build single-block blocked document file", "[v1][unit]")
{
    using sink_type = boost::iostreams::back_insert_device<std::vector<std::byte>>;
    using vector_stream_type = boost::iostreams::stream<sink_type>;

    std::vector<DocId> docids{3, 4, 5, 6, 7, 8, 9, 10, 51, 115};
    std::vector<std::byte> docbuf;
    auto document_offsets = [&]() {
        PostingBuilder<DocId> document_builder(DocumentBlockedWriter<pisa::simdbp_block>{});
        vector_stream_type docstream{sink_type{docbuf}};
        document_builder.write_header(docstream);
        document_builder.write_segment(docstream, docids.begin(), docids.end());
        return document_builder.offsets();
    }();

    auto documents = gsl::span<std::byte const>(docbuf).subspan(8);
    CHECK(docbuf.size() == document_offsets.back() + 8);
    DocumentBlockedReader<pisa::simdbp_block> document_reader;
    auto term = 0;
    auto actual = collect(document_reader.read(documents.subspan(
        document_offsets[term], document_offsets[term + 1] - document_offsets[term])));
    CHECK(actual.size() == docids.size());
    REQUIRE(actual == docids);
}

TEST_CASE("Build blocked document-frequency index", "[v1][unit]")
{
    using sink_type = boost::iostreams::back_insert_device<std::vector<std::byte>>;
    using vector_stream_type = boost::iostreams::stream<sink_type>;
    GIVEN("A test binary collection")
    {
        pisa::binary_freq_collection collection(PISA_SOURCE_DIR "/test/test_data/test_collection");
        WHEN("Built posting files for documents and frequencies")
        {
            std::vector<std::byte> docbuf;
            std::vector<std::byte> freqbuf;

            PostingBuilder<DocId> document_builder(DocumentBlockedWriter<pisa::simdbp_block>{});
            PostingBuilder<Frequency> frequency_builder(PayloadBlockedWriter<pisa::simdbp_block>{});
            {
                vector_stream_type docstream{sink_type{docbuf}};
                vector_stream_type freqstream{sink_type{freqbuf}};

                document_builder.write_header(docstream);
                frequency_builder.write_header(freqstream);

                for (auto sequence : collection) {
                    document_builder.write_segment(
                        docstream, sequence.docs.begin(), sequence.docs.end());
                    frequency_builder.write_segment(
                        freqstream, sequence.freqs.begin(), sequence.freqs.end());
                }
            }

            auto document_offsets = document_builder.offsets();
            auto frequency_offsets = frequency_builder.offsets();

            auto document_sizes = read_sizes(PISA_SOURCE_DIR "/test/test_data/test_collection");
            auto documents = gsl::span<std::byte const>(docbuf).subspan(8);
            auto frequencies = gsl::span<std::byte const>(freqbuf).subspan(8);

            THEN("The values read back are euqual to the binary collection's")
            {
                CHECK(docbuf.size() == document_offsets.back() + 8);
                DocumentBlockedReader<pisa::simdbp_block> document_reader;
                PayloadBlockedReader<pisa::simdbp_block> frequency_reader;
                auto term = 0;
                std::for_each(collection.begin(), collection.end(), [&](auto&& seq) {
                    std::vector<DocId> expected_documents(seq.docs.begin(), seq.docs.end());
                    auto actual_documents = collect(document_reader.read(
                        documents.subspan(document_offsets[term],
                                          document_offsets[term + 1] - document_offsets[term])));
                    CHECK(actual_documents.size() == expected_documents.size());
                    REQUIRE(actual_documents == expected_documents);

                    std::vector<DocId> expected_frequencies(seq.freqs.begin(), seq.freqs.end());
                    auto actual_frequencies = collect(frequency_reader.read(frequencies.subspan(
                        frequency_offsets[term],
                        frequency_offsets[term + 1] - frequency_offsets[term])));
                    CHECK(actual_frequencies.size() == expected_frequencies.size());
                    REQUIRE(actual_frequencies == expected_frequencies);
                    term += 1;
                });
            }

            THEN("Index runner is correctly constructed")
            {
                auto source = std::array<std::vector<std::byte>, 2>{docbuf, freqbuf};
                auto document_span = gsl::span<std::byte const>(
                    reinterpret_cast<std::byte const*>(source[0].data()), source[0].size());
                auto payload_span = gsl::span<std::byte const>(
                    reinterpret_cast<std::byte const*>(source[1].data()), source[1].size());

                IndexRunner runner(PostingData{document_span, document_offsets},
                                   PostingData{payload_span, frequency_offsets},
                                   {},
                                   document_sizes,
                                   tl::nullopt,
                                   {},
                                   {},
                                   {},
                                   std::move(source),
                                   std::make_tuple(DocumentBlockedReader<pisa::simdbp_block>{}),
                                   std::make_tuple(PayloadBlockedReader<pisa::simdbp_block>{}));
                int counter = 0;
                runner([&](auto index) {
                    counter += 1;
                    TermId term_id = 0;
                    for (auto sequence : collection) {
                        CAPTURE(term_id);
                        REQUIRE(sequence.docs.size() == index.cursor(term_id).size());
                        REQUIRE(
                            std::vector<std::uint32_t>(sequence.docs.begin(), sequence.docs.end())
                            == collect(index.cursor(term_id)));
                        REQUIRE(
                            std::vector<std::uint32_t>(sequence.freqs.begin(), sequence.freqs.end())
                            == collect(index.cursor(term_id),
                                       [](auto&& cursor) { return cursor.payload(); }));
                        {
                            auto cursor = index.cursor(term_id);
                            for (auto doc : sequence.docs) {
                                cursor.advance_to_geq(doc);
                                REQUIRE(cursor.value() == doc);
                            }
                        }
                        {
                            auto cursor = index.cursor(term_id);
                            for (auto doc : sequence.docs) {
                                REQUIRE(cursor.value() == doc);
                                cursor.advance_to_geq(doc + 1);
                            }
                        }
                        term_id += 1;
                    }
                });
                REQUIRE(counter == 1);
            }

            THEN("Index runner fails when wrong type")
            {
                auto source = std::array<std::vector<std::byte>, 2>{docbuf, freqbuf};
                auto document_span = gsl::span<std::byte const>(
                    reinterpret_cast<std::byte const*>(source[0].data()), source[0].size());
                auto payload_span = gsl::span<std::byte const>(
                    reinterpret_cast<std::byte const*>(source[1].data()), source[1].size());
                IndexRunner runner(PostingData{document_span, document_offsets},
                                   PostingData{payload_span, frequency_offsets},
                                   {},
                                   document_sizes,
                                   tl::nullopt,
                                   {},
                                   {},
                                   {},
                                   std::move(source),
                                   std::make_tuple(RawReader<std::uint32_t>{}),
                                   std::make_tuple());
                REQUIRE_THROWS_AS(runner([&](auto index) {}), std::domain_error);
            }
        }
    }
}
