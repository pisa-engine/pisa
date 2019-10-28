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
#include "v1/posting_builder.hpp"
#include "v1/posting_format_header.hpp"
#include "v1/types.hpp"

using pisa::v1::Array;
using pisa::v1::DocId;
using pisa::v1::Frequency;
using pisa::v1::IndexRunner;
using pisa::v1::parse_type;
using pisa::v1::PostingBuilder;
using pisa::v1::PostingFormatHeader;
using pisa::v1::Primitive;
using pisa::v1::RawReader;
using pisa::v1::RawWriter;
using pisa::v1::TermId;
using pisa::v1::Tuple;
using pisa::v1::Writer;

template <typename T>
std::ostream &operator<<(std::ostream &os, tl::optional<T> const &val)
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
    REQUIRE(cursor.value().value() == tl::make_optional(mem[1]).value());
    REQUIRE(cursor.next() == tl::make_optional(mem[2]));
    REQUIRE(cursor.next() == tl::make_optional(mem[3]));
    REQUIRE(cursor.next() == tl::make_optional(mem[4]));
    REQUIRE(cursor.next() == tl::make_optional(mem[5]));
    REQUIRE(cursor.next() == tl::nullopt);
}

TEST_CASE("Binary collection index", "[v1][unit]")
{
    pisa::binary_freq_collection collection(PISA_SOURCE_DIR "/test/test_data/test_collection");
    auto index =
        pisa::v1::binary_collection_index(PISA_SOURCE_DIR "/test/test_data/test_collection");
    auto term_id = 0;
    for (auto sequence : collection) {
        CAPTURE(term_id);
        REQUIRE(std::vector<std::uint32_t>(sequence.docs.begin(), sequence.docs.end())
                == collect(index.documents(term_id)));
        REQUIRE(std::vector<std::uint32_t>(sequence.freqs.begin(), sequence.freqs.end())
                == collect(index.payloads(term_id)));
        term_id += 1;
    }
    term_id = 0;
    for (auto sequence : collection) {
        CAPTURE(term_id);
        REQUIRE(std::vector<std::uint32_t>(sequence.docs.begin(), sequence.docs.end())
                == collect(index.cursor(term_id)));
        REQUIRE(std::vector<std::uint32_t>(sequence.freqs.begin(), sequence.freqs.end())
                == collect(index.cursor(term_id), [](auto &&cursor) { return *cursor.payload(); }));
        term_id += 1;
    }
}

TEST_CASE("Bigram collection index", "[v1][unit]")
{
    auto intersect = [](auto const &lhs,
                        auto const &rhs) -> std::vector<std::tuple<DocId, Frequency, Frequency>> {
        std::vector<std::tuple<DocId, Frequency, Frequency>> intersection;
        auto left = lhs.begin();
        auto right = rhs.begin();
        while (left != lhs.end() && right != rhs.end()) {
            if (left->first == right->first) {
                intersection.emplace_back(left->first, left->second, right->second);
                ++right;
                ++left;
            } else if (left->first < right->first) {
                ++left;
            } else {
                ++right;
            }
        }
        return intersection;
    };
    auto to_vec = [](auto const &seq) {
        std::vector<std::pair<DocId, Frequency>> vec;
        std::transform(seq.docs.begin(),
                       seq.docs.end(),
                       seq.freqs.begin(),
                       std::back_inserter(vec),
                       [](auto doc, auto freq) { return std::make_pair(doc, freq); });
        return vec;
    };

    pisa::binary_freq_collection collection(PISA_SOURCE_DIR "/test/test_data/test_collection");
    auto index =
        pisa::v1::binary_collection_bigram_index(PISA_SOURCE_DIR "/test/test_data/test_collection");

    auto pos = collection.begin();
    auto prev = to_vec(*pos);
    ++pos;
    TermId term_id = 1;
    for (; pos != collection.end(); ++pos, ++term_id) {
        auto current = to_vec(*pos);
        auto intersection = intersect(prev, current);
        if (not intersection.empty()) {
            auto id = index.bigram_id(term_id - 1, term_id);
            REQUIRE(id.has_value());
            auto postings = collect(index.cursor(*id), [](auto &cursor) {
                auto freqs = *cursor.payload();
                return std::make_tuple(*cursor, freqs[0], freqs[1]);
            });
            REQUIRE(postings == intersection);
        }
        std::swap(prev, current);
        break;
    }
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
    using sink_type = boost::iostreams::back_insert_device<std::vector<char>>;
    using vector_stream_type = boost::iostreams::stream<sink_type>;
    GIVEN("A test binary collection")
    {
        pisa::binary_freq_collection collection(PISA_SOURCE_DIR "/test/test_data/test_collection");
        WHEN("Built posting files for documents and frequencies")
        {
            std::vector<char> docbuf;
            std::vector<char> freqbuf;

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

            THEN("Bytes match with those of the collection")
            {
                auto document_bytes =
                    pisa::io::load_data(PISA_SOURCE_DIR "/test/test_data/test_collection.docs");
                auto frequency_bytes =
                    pisa::io::load_data(PISA_SOURCE_DIR "/test/test_data/test_collection.freqs");

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
                auto source = std::array<std::vector<char>, 2>{docbuf, freqbuf};
                auto document_span = gsl::span<std::byte const>(
                    reinterpret_cast<std::byte const *>(source[0].data()), source[0].size());
                auto payload_span = gsl::span<std::byte const>(
                    reinterpret_cast<std::byte const *>(source[1].data()), source[1].size());
                IndexRunner runner(document_offsets,
                                   frequency_offsets,
                                   document_span,
                                   payload_span,
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
                                       [](auto &&cursor) { return *cursor.payload(); }));
                        term_id += 1;
                    }
                });
                REQUIRE(counter == 1);
            }

            THEN("Index runner fails when wrong type")
            {
                auto source = std::array<std::vector<char>, 2>{docbuf, freqbuf};
                auto document_span = gsl::span<std::byte const>(
                    reinterpret_cast<std::byte const *>(source[0].data()), source[0].size());
                auto payload_span = gsl::span<std::byte const>(
                    reinterpret_cast<std::byte const *>(source[1].data()), source[1].size());
                IndexRunner runner(document_offsets,
                                   frequency_offsets,
                                   document_span,
                                   payload_span,
                                   std::move(source),
                                   RawReader<float>{}); // Correct encoding but not type!
                REQUIRE_THROWS_AS(runner([&](auto index) {}), std::domain_error);
            }
        }
    }
}
