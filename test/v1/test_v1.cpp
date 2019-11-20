#define CATCH_CONFIG_MAIN
#include "catch2/catch.hpp"

#include <string>

#include <boost/iostreams/device/back_inserter.hpp>
#include <boost/iostreams/stream.hpp>
#include <tbb/task_scheduler_init.h>

#include "io.hpp"
#include "pisa_config.hpp"
#include "v1/cursor/collect.hpp"
#include "v1/index.hpp"
#include "v1/io.hpp"
#include "v1/posting_builder.hpp"
#include "v1/posting_format_header.hpp"
#include "v1/scorer/bm25.hpp"
#include "v1/scorer/runner.hpp"
#include "v1/types.hpp"
#include "v1/unaligned_span.hpp"

using pisa::v1::Array;
using pisa::v1::DocId;
using pisa::v1::Frequency;
using pisa::v1::IndexRunner;
using pisa::v1::load_bytes;
using pisa::v1::next;
using pisa::v1::parse_type;
using pisa::v1::PostingBuilder;
using pisa::v1::PostingFormatHeader;
using pisa::v1::Primitive;
using pisa::v1::RawReader;
using pisa::v1::RawWriter;
using pisa::v1::read_sizes;
using pisa::v1::TermId;
using pisa::v1::Tuple;
using pisa::v1::UnalignedSpan;
using pisa::v1::Writer;

template <typename T>
std::ostream& operator<<(std::ostream& os, tl::optional<T> const& val)
{
    if (val.has_value()) {
        os << val.value();
    } else {
        os << "nullopt";
    }
    return os;
}

TEST_CASE("RawReader", "[v1][unit]")
{
    std::vector<std::uint32_t> const mem{5, 0, 1, 2, 3, 4};
    RawReader<uint32_t> reader;
    auto cursor = reader.read(gsl::as_bytes(gsl::make_span(mem)));
    REQUIRE(cursor.value() == mem[1]);
    REQUIRE(next(cursor) == tl::make_optional(mem[2]));
    REQUIRE(next(cursor) == tl::make_optional(mem[3]));
    REQUIRE(next(cursor) == tl::make_optional(mem[4]));
    REQUIRE(next(cursor) == tl::make_optional(mem[5]));
    REQUIRE(next(cursor) == tl::nullopt);
}

TEST_CASE("Test read header", "[v1][unit]")
{
    {
        std::vector<std::byte> bytes{
            std::byte{0b00000000},
            std::byte{0b00000001},
            std::byte{0b00000000},
            std::byte{0b00000000},
            std::byte{0b00000000},
            std::byte{0b00000000},
            std::byte{0b00000000},
            std::byte{0b00000000},
        };
        auto header = PostingFormatHeader::parse(gsl::span<std::byte const>(bytes));
        REQUIRE(header.version.major == 0);
        REQUIRE(header.version.minor == 1);
        REQUIRE(header.version.patch == 0);
        REQUIRE(std::get<Primitive>(header.type) == Primitive::Int);
        REQUIRE(header.encoding == 0);
    }
    {
        std::vector<std::byte> bytes{
            std::byte{0b00000001},
            std::byte{0b00000001},
            std::byte{0b00000011},
            std::byte{0b00000001},
            std::byte{0b00000001},
            std::byte{0b00000000},
            std::byte{0b00000000},
            std::byte{0b00000000},
        };
        auto header = PostingFormatHeader::parse(gsl::span<std::byte const>(bytes));
        REQUIRE(header.version.major == 1);
        REQUIRE(header.version.minor == 1);
        REQUIRE(header.version.patch == 3);
        REQUIRE(std::get<Primitive>(header.type) == Primitive::Float);
        REQUIRE(header.encoding == 1);
    }
    {
        std::vector<std::byte> bytes{
            std::byte{0b00000001},
            std::byte{0b00000000},
            std::byte{0b00000011},
            std::byte{0b00000010},
            std::byte{0b00000011},
            std::byte{0b00000000},
            std::byte{0b00000000},
            std::byte{0b00000000},
        };
        auto header = PostingFormatHeader::parse(gsl::span<std::byte const>(bytes));
        REQUIRE(header.version.major == 1);
        REQUIRE(header.version.minor == 0);
        REQUIRE(header.version.patch == 3);
        REQUIRE(std::get<Array>(header.type).type == Primitive::Int);
        REQUIRE(header.encoding == 3);
    }
}

TEST_CASE("Value type", "[v1][unit]")
{
    REQUIRE(std::get<Primitive>(parse_type(std::byte{0b00000000})) == Primitive::Int);
    REQUIRE(std::get<Primitive>(parse_type(std::byte{0b00000001})) == Primitive::Float);
    REQUIRE(std::get<Array>(parse_type(std::byte{0b00000010})).type == Primitive::Int);
    REQUIRE(std::get<Array>(parse_type(std::byte{0b00000110})).type == Primitive::Float);
    REQUIRE(std::get<Tuple>(parse_type(std::byte{0b00101011})).type == Primitive::Int);
    REQUIRE(std::get<Tuple>(parse_type(std::byte{0b01000111})).type == Primitive::Float);
    REQUIRE(std::get<Tuple>(parse_type(std::byte{0b00101011})).size == 5U);
    REQUIRE(std::get<Tuple>(parse_type(std::byte{0b01000111})).size == 8U);
}

TEST_CASE("Build raw document-frequency index", "[v1][unit]")
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

            PostingBuilder<DocId> document_builder(Writer<DocId>(RawWriter<DocId>{}));
            PostingBuilder<Frequency> frequency_builder(Writer<DocId>(RawWriter<Frequency>{}));
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

            THEN("Bytes match with those of the collection")
            {
                auto document_bytes =
                    load_bytes(PISA_SOURCE_DIR "/test/test_data/test_collection.docs");
                auto frequency_bytes =
                    load_bytes(PISA_SOURCE_DIR "/test/test_data/test_collection.freqs");

                // NOTE: the first 8 bytes of the document collection are different than those
                // of the built document file. Also, the original frequency collection starts
                // at byte 0 (no 8-byte "size vector" at the beginning), and thus is shorter.
                CHECK(docbuf.size() == document_offsets.back() + 8);
                CHECK(freqbuf.size() == frequency_offsets.back() + 8);
                CHECK(docbuf.size() == document_bytes.size());
                CHECK(freqbuf.size() == frequency_bytes.size() + 8);
                CHECK(gsl::make_span(docbuf.data(), docbuf.size()).subspan(8)
                      == gsl::make_span(document_bytes.data(), document_bytes.size()).subspan(8));
                CHECK(gsl::make_span(freqbuf.data(), freqbuf.size()).subspan(8)
                      == gsl::make_span(frequency_bytes.data(), frequency_bytes.size()));
            }

            THEN("Index runner is correctly constructed")
            {
                auto source = std::array<std::vector<std::byte>, 2>{docbuf, freqbuf};
                auto document_span = gsl::span<std::byte const>(
                    reinterpret_cast<std::byte const*>(source[0].data()), source[0].size());
                auto payload_span = gsl::span<std::byte const>(
                    reinterpret_cast<std::byte const*>(source[1].data()), source[1].size());

                IndexRunner runner(document_offsets,
                                   frequency_offsets,
                                   {},
                                   {},
                                   document_span,
                                   payload_span,
                                   {},
                                   {},
                                   document_sizes,
                                   tl::nullopt,
                                   {},
                                   {},
                                   tl::nullopt,
                                   std::move(source),
                                   RawReader<std::uint32_t>{},
                                   RawReader<std::uint32_t>{}); // Repeat to test that it only
                                                                // executes once
                int counter = 0;
                runner([&](auto index) {
                    counter += 1;
                    TermId term_id = 0;
                    for (auto sequence : collection) {
                        CAPTURE(term_id);
                        REQUIRE(
                            std::vector<std::uint32_t>(sequence.docs.begin(), sequence.docs.end())
                            == collect(index.cursor(term_id)));
                        REQUIRE(
                            std::vector<std::uint32_t>(sequence.freqs.begin(), sequence.freqs.end())
                            == collect(index.cursor(term_id),
                                       [](auto&& cursor) { return cursor.payload(); }));
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
                IndexRunner runner(document_offsets,
                                   frequency_offsets,
                                   {},
                                   {},
                                   document_span,
                                   payload_span,
                                   {},
                                   {},
                                   document_sizes,
                                   tl::nullopt,
                                   {},
                                   {},
                                   tl::nullopt,
                                   std::move(source),
                                   RawReader<float>{}); // Correct encoding but not type!
                REQUIRE_THROWS_AS(runner([&](auto index) {}), std::domain_error);
            }
        }
    }
}

TEST_CASE("UnalignedSpan", "[v1][unit]")
{
    std::vector<std::byte> bytes{std::byte{0b00000001},
                                 std::byte{0b00000010},
                                 std::byte{0b00000011},
                                 std::byte{0b00000100},
                                 std::byte{0b00000101},
                                 std::byte{0b00000110},
                                 std::byte{0b00000111}};
    SECTION("Bytes one-to-one")
    {
        auto span = UnalignedSpan<std::byte>(gsl::make_span(bytes));
        REQUIRE(std::vector<std::byte>(span.begin(), span.end()) == bytes);
    }
    SECTION("Bytes shifted by offset")
    {
        auto span = UnalignedSpan<std::byte>(gsl::make_span(bytes).subspan(2));
        REQUIRE(std::vector<std::byte>(span.begin(), span.end())
                == std::vector<std::byte>(bytes.begin() + 2, bytes.end()));
    }
    SECTION("u16")
    {
        REQUIRE_THROWS_AS(UnalignedSpan<std::uint16_t>(gsl::make_span(bytes)), std::logic_error);
        auto span = UnalignedSpan<std::uint16_t>(gsl::make_span(bytes).subspan(1));
        REQUIRE(std::vector<std::uint16_t>(span.begin(), span.end())
                == std::vector<std::uint16_t>{
                    0b0000001100000010, 0b0000010100000100, 0b0000011100000110});
    }
    SECTION("u32")
    {
        REQUIRE_THROWS_AS(UnalignedSpan<std::uint32_t>(gsl::make_span(bytes)), std::logic_error);
        auto span = UnalignedSpan<std::uint32_t>(gsl::make_span(bytes).subspan(1, 4));
        REQUIRE(std::vector<std::uint32_t>(span.begin(), span.end())
                == std::vector<std::uint32_t>{0b00000101000001000000001100000010});
    }
}
