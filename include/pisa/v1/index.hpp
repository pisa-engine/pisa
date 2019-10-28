#pragma once

#include <any>
#include <cstdint>
#include <cstring>
#include <type_traits>
#include <utility>

#include <boost/iostreams/device/back_inserter.hpp>
#include <boost/iostreams/stream.hpp>
#include <boost/iterator/counting_iterator.hpp>
#include <fmt/format.h>
#include <gsl/gsl_assert>
#include <gsl/span>
#include <mio/mmap.hpp>
#include <tl/optional.hpp>

#include "binary_freq_collection.hpp"
#include "v1/bit_cast.hpp"
#include "v1/cursor/for_each.hpp"
#include "v1/cursor_intersection.hpp"
#include "v1/document_payload_cursor.hpp"
#include "v1/posting_builder.hpp"
#include "v1/raw_cursor.hpp"
#include "v1/types.hpp"

namespace pisa::v1 {

/// A generic type for an inverted index.
///
/// \tparam DocumentReader  Type of an object that reads document posting lists from bytes
///                         It must read lists containing `DocId` objects.
/// \tparam PayloadReader   Type of an object that reads payload posting lists from bytes.
///                         It can read lists of arbitrary types, such as `Frequency`,
///                         `Score`, or `std::pair<Score, Score>` for a bigram scored index.
template <typename DocumentCursor, typename PayloadCursor>
struct Index {
    /// Constructs the index.
    ///
    /// \param document_reader  Reads document posting lists from bytes.
    /// \param payload_reader   Reads payload posting lists from bytes.
    /// \param document_offsets Mapping from term ID to the position in memory of its
    ///                         document posting list.
    /// \param payload_offsets  Mapping from term ID to the position in memory of its
    ///                         payload posting list.
    /// \param documents        Encoded bytes for document postings.
    /// \param payloads         Encoded bytes for payload postings.
    /// \param source           This object (optionally) owns the raw data pointed at by
    ///                         `documents` and `payloads` to ensure it is valid throughout
    ///                         the lifetime of the index. It should release any resources
    ///                         in its destructor.
    template <typename DocumentReader, typename PayloadReader, typename Source>
    Index(DocumentReader document_reader,
          PayloadReader payload_reader,
          std::vector<std::size_t> document_offsets,
          std::vector<std::size_t> payload_offsets,
          gsl::span<std::byte const> documents,
          gsl::span<std::byte const> payloads,
          Source source)
        : m_document_reader(std::move(document_reader)),
          m_payload_reader(std::move(payload_reader)),
          m_document_offsets(std::move(document_offsets)),
          m_payload_offsets(std::move(payload_offsets)),
          m_documents(documents),
          m_payloads(payloads),
          m_source(std::move(source))
    {
    }

    /// Constructs a new document-payload cursor (see document_payload_cursor.hpp).
    [[nodiscard]] auto cursor(TermId term)
    {
        return DocumentPayloadCursor<DocumentCursor, PayloadCursor>(documents(term),
                                                                    payloads(term));
    }

    /// Constructs a new document cursor.
    [[nodiscard]] auto documents(TermId term)
    {
        return m_document_reader.read(fetch_documents(term));
    }

    /// Constructs a new payload cursor.
    [[nodiscard]] auto payloads(TermId term) { return m_payload_reader.read(fetch_payloads(term)); }

    /// Constructs a new payload cursor.
    [[nodiscard]] auto num_terms() -> std::uint32_t { return m_document_offsets.size() - 1; }

   private:
    [[nodiscard]] auto fetch_documents(TermId term) -> gsl::span<std::byte const>
    {
        Expects(term + 1 < m_document_offsets.size());
        return m_documents.subspan(m_document_offsets[term],
                                   m_document_offsets[term + 1] - m_document_offsets[term]);
    }
    [[nodiscard]] auto fetch_payloads(TermId term) -> gsl::span<std::byte const>
    {
        Expects(term + 1 < m_payload_offsets.size());
        return m_payloads.subspan(m_payload_offsets[term],
                                  m_payload_offsets[term + 1] - m_payload_offsets[term]);
    }

    Reader<DocumentCursor> m_document_reader;
    Reader<PayloadCursor> m_payload_reader;
    std::vector<std::size_t> m_document_offsets;
    std::vector<std::size_t> m_payload_offsets;
    gsl::span<std::byte const> m_documents;
    gsl::span<std::byte const> m_payloads;
    std::any m_source;
};

template <typename DocumentReader, typename PayloadReader, typename Source>
auto make_index(DocumentReader document_reader,
                PayloadReader payload_reader,
                std::vector<std::size_t> document_offsets,
                std::vector<std::size_t> payload_offsets,
                gsl::span<std::byte const> documents,
                gsl::span<std::byte const> payloads,
                Source source)
{
    using DocumentCursor =
        decltype(document_reader.read(std::declval<gsl::span<std::byte const>>()));
    using PayloadCursor = decltype(payload_reader.read(std::declval<gsl::span<std::byte const>>()));
    return Index<DocumentCursor, PayloadCursor>(std::move(document_reader),
                                                std::move(payload_reader),
                                                std::move(document_offsets),
                                                std::move(payload_offsets),
                                                documents,
                                                payloads,
                                                std::move(source));
}

/// Initializes a memory mapped source with a given file.
inline void open_source(mio::mmap_source &source, std::string const &filename)
{
    std::error_code error;
    source.map(filename, error);
    if (error) {
        spdlog::error("Error mapping file {}: {}", filename, error.message());
        throw std::runtime_error("Error mapping file");
    }
}

[[nodiscard]] inline auto binary_collection_index(std::string const &basename)
{
    using sink_type = boost::iostreams::back_insert_device<std::vector<char>>;
    using vector_stream_type = boost::iostreams::stream<sink_type>;

    binary_freq_collection collection(basename.c_str());
    std::vector<char> docbuf;
    std::vector<char> freqbuf;

    PostingBuilder<DocId> document_builder(Writer<DocId>(RawWriter<DocId>{}));
    PostingBuilder<Frequency> frequency_builder(Writer<Frequency>(RawWriter<Frequency>{}));
    {
        vector_stream_type docstream{sink_type{docbuf}};
        vector_stream_type freqstream{sink_type{freqbuf}};

        document_builder.write_header(docstream);
        frequency_builder.write_header(freqstream);

        for (auto sequence : collection) {
            document_builder.write_segment(docstream, sequence.docs.begin(), sequence.docs.end());
            frequency_builder.write_segment(
                freqstream, sequence.freqs.begin(), sequence.freqs.end());
        }
    }

    auto document_offsets = document_builder.offsets();
    auto frequency_offsets = frequency_builder.offsets();
    auto source = std::make_shared<std::pair<std::vector<char>, std::vector<char>>>(
        std::move(docbuf), std::move(freqbuf));
    auto documents = gsl::span<std::byte const>(
        reinterpret_cast<std::byte const *>(source->first.data()), source->first.size());
    auto frequencies = gsl::span<std::byte const>(
        reinterpret_cast<std::byte const *>(source->second.data()), source->second.size());

    return Index<RawCursor<DocId>, RawCursor<Frequency>>(RawReader<DocId>{},
                                                         RawReader<Frequency>{},
                                                         document_offsets,
                                                         frequency_offsets,
                                                         documents.subspan(8),
                                                         frequencies.subspan(8),
                                                         std::move(source));
}

template <typename... Readers>
struct IndexRunner {
    template <typename Source>
    IndexRunner(std::vector<std::size_t> document_offsets,
                std::vector<std::size_t> payload_offsets,
                gsl::span<std::byte const> documents,
                gsl::span<std::byte const> payloads,
                Source source,
                Readers... readers)
        : m_document_offsets(std::move(document_offsets)),
          m_payload_offsets(std::move(payload_offsets)),
          m_documents(documents),
          m_payloads(payloads),
          m_source(std::move(source)),
          m_readers(readers...)
    {
    }

    template <typename Fn>
    void operator()(Fn fn)
    {
        auto dheader = PostingFormatHeader::parse(m_documents.first(8));
        auto pheader = PostingFormatHeader::parse(m_payloads.first(8));
        auto run = [&](auto &&dreader, auto &&preader) -> bool {
            if (std::decay_t<decltype(dreader)>::encoding() == dheader.encoding
                && std::decay_t<decltype(preader)>::encoding() == pheader.encoding
                && is_type<typename std::decay_t<decltype(dreader)>::value_type>(dheader.type)
                && is_type<typename std::decay_t<decltype(preader)>::value_type>(pheader.type)) {
                fn(make_index(std::forward<decltype(dreader)>(dreader),
                              std::forward<decltype(preader)>(preader),
                              m_document_offsets,
                              m_payload_offsets,
                              m_documents.subspan(8),
                              m_payloads.subspan(8),
                              false));
                return true;
            }
            return false;
        };
        bool success = std::apply(
            [&](Readers... dreaders) {
                auto with_document_reader = [&](auto dreader) {
                    return std::apply(
                        [&](Readers... preaders) { return (run(dreader, preaders) || ...); },
                        m_readers);
                };
                return (with_document_reader(dreaders) || ...);
            },
            m_readers);
        if (not success) {
            throw std::domain_error("Unknown posting encoding");
        }
    }

   private:
    std::vector<std::size_t> m_document_offsets;
    std::vector<std::size_t> m_payload_offsets;
    gsl::span<std::byte const> m_documents;
    gsl::span<std::byte const> m_payloads;
    std::any m_source;
    std::tuple<Readers...> m_readers;
};

template <typename Index>
struct BigramIndex : public Index {
    using PairMapping = std::vector<std::pair<TermId, TermId>>;

    BigramIndex(Index index, PairMapping pair_mapping)
        : Index(std::move(index)), m_pair_mapping(std::move(pair_mapping))
    {
    }

    [[nodiscard]] auto bigram_id(TermId left, TermId right) -> tl::optional<TermId>
    {
        auto pos =
            std::find(m_pair_mapping.begin(), m_pair_mapping.end(), std::make_pair(left, right));
        if (pos != m_pair_mapping.end()) {
            return tl::make_optional(std::distance(m_pair_mapping.begin(), pos));
        }
        return tl::nullopt;
    }

   private:
    PairMapping m_pair_mapping;
};

/// Creates, on the fly, a bigram index with all pairs of adjecent terms.
/// Disclaimer: for testing purposes.
[[nodiscard]] inline auto binary_collection_bigram_index(std::string const &basename)
{
    using payload_type = std::array<Frequency, 2>;
    using sink_type = boost::iostreams::back_insert_device<std::vector<char>>;
    using vector_stream_type = boost::iostreams::stream<sink_type>;

    auto unigram_index = binary_collection_index(basename);

    std::vector<std::pair<TermId, TermId>> pair_mapping;
    std::vector<char> docbuf;
    std::vector<char> freqbuf;

    PostingBuilder<DocId> document_builder(RawWriter<DocId>{});
    PostingBuilder<payload_type> frequency_builder(RawWriter<payload_type>{});
    {
        vector_stream_type docstream{sink_type{docbuf}};
        vector_stream_type freqstream{sink_type{freqbuf}};

        document_builder.write_header(docstream);
        frequency_builder.write_header(freqstream);

        std::for_each(boost::counting_iterator<TermId>(0),
                      boost::counting_iterator<TermId>(unigram_index.num_terms() - 1),
                      [&](auto left) {
                          auto right = left + 1;
                          auto intersection = CursorIntersection(
                              std::vector{unigram_index.cursor(left), unigram_index.cursor(right)},
                              payload_type{0, 0},
                              [](payload_type &payload, auto &cursor, auto list_idx) {
                                  payload[list_idx] = cursor.payload();
                                  return payload;
                              });
                          if (intersection.empty()) {
                              // Include only non-empty intersections.
                              return;
                          }
                          pair_mapping.emplace_back(left, right);
                          for_each(intersection, [&](auto &cursor) {
                              document_builder.accumulate(*cursor);
                              frequency_builder.accumulate(cursor.payload());
                          });
                          document_builder.flush_segment(docstream);
                          frequency_builder.flush_segment(freqstream);
                      });
    }

    auto source = std::array<std::vector<char>, 2>{std::move(docbuf), std::move(freqbuf)};
    auto document_span = gsl::span<std::byte const>(
        reinterpret_cast<std::byte const *>(source[0].data()), source[0].size());
    auto payload_span = gsl::span<std::byte const>(
        reinterpret_cast<std::byte const *>(source[1].data()), source[1].size());
    auto index = Index<RawCursor<DocId>, RawCursor<payload_type>>(RawReader<DocId>{},
                                                                  RawReader<payload_type>{},
                                                                  document_builder.offsets(),
                                                                  frequency_builder.offsets(),
                                                                  document_span.subspan(8),
                                                                  payload_span.subspan(8),
                                                                  std::move(source));
    return BigramIndex(std::move(index), std::move(pair_mapping));
}

} // namespace pisa::v1
