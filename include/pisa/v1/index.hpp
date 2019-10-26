#pragma once

#include <cstdint>
#include <cstring>
#include <type_traits>
#include <utility>

#include <gsl/gsl_assert>
#include <gsl/span>
#include <mio/mmap.hpp>
#include <tl/optional.hpp>

#include "binary_freq_collection.hpp"
#include "v1/bit_cast.hpp"
#include "v1/cursor_intersection.hpp"
#include "v1/document_payload_cursor.hpp"
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
/// \tparam Source          Can be used to store any owning data, like open `mmap`, since
///                         index internally uses spans to manage encoded parts of memory.
template <typename DocumentReader, typename PayloadReader, typename Source>
struct Index {

    /// The type of cursor constructed by the document reader. Must read `DocId` values.
    using DocumentCursor =
        decltype(std::declval<DocumentReader>().read(std::declval<gsl::span<std::byte const>>()));
    static_assert(std::is_same_v<decltype(*std::declval<DocumentCursor>()), DocId>);

    /// The type of cursor constructed by the payload reader.
    using PayloadCursor =
        decltype(std::declval<PayloadReader>().read(std::declval<gsl::span<std::byte const>>()));

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
    ///                         the lifetime of the index.
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
        return m_document_reader.read(fetch_documents(term).subspan(4));
    }

    /// Constructs a new payload cursor.
    [[nodiscard]] auto payloads(TermId term)
    {
        return m_payload_reader.read(fetch_payloads(term).subspan(4));
    }

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

    DocumentReader m_document_reader;
    PayloadReader m_payload_reader;
    std::vector<std::size_t> m_document_offsets;
    std::vector<std::size_t> m_payload_offsets;
    gsl::span<std::byte const> m_documents;
    gsl::span<std::byte const> m_payloads;
    Source m_source;
};

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
    binary_freq_collection collection(basename.c_str());
    std::vector<std::size_t> document_offsets;
    std::vector<std::size_t> frequency_offsets;
    document_offsets.push_back(8);
    frequency_offsets.push_back(0);
    for (auto const &postings : collection) {
        auto offset = (1 + postings.docs.size()) * sizeof(std::uint32_t);
        document_offsets.push_back(document_offsets.back() + offset);
        frequency_offsets.push_back(frequency_offsets.back() + offset);
    }
    auto source = std::make_unique<std::pair<mio::mmap_source, mio::mmap_source>>();
    open_source(source->first, basename + ".docs");
    open_source(source->second, basename + ".freqs");
    auto documents = gsl::make_span<std::byte const>(
        reinterpret_cast<std::byte const *>(source->first.data()), source->first.size());
    auto frequencies = gsl::make_span<std::byte const>(
        reinterpret_cast<std::byte const *>(source->second.data()), source->second.size());
    return Index<RawReader<DocId>,
                 RawReader<Frequency>,
                 std::unique_ptr<std::pair<mio::mmap_source, mio::mmap_source>>>(
        {},
        {},
        std::move(document_offsets),
        std::move(frequency_offsets),
        documents,
        frequencies,
        std::move(source));
}

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

    auto unigram_index = binary_collection_index(basename);

    std::vector<std::pair<TermId, TermId>> pair_mapping;
    std::vector<std::byte> documents;
    std::vector<std::byte> payloads;

    std::vector<std::size_t> document_offsets;
    std::vector<std::size_t> payload_offsets;

    {
        // Hack to be backwards-compatible with binary_freq_collection (for now).
        documents.insert(documents.begin(), 8, std::byte{0});
    }

    document_offsets.push_back(documents.size());
    payload_offsets.push_back(payloads.size());
    for (TermId left = 0; left < unigram_index.num_terms() - 1; left += 1) {
        auto right = left + 1;
        RawWriter<DocId> document_writer;
        RawWriter<payload_type> payload_writer;
        auto inter =
            CursorIntersection(std::vector{unigram_index.cursor(left), unigram_index.cursor(right)},
                               payload_type{0, 0},
                               [](payload_type &payload, auto &cursor, auto list_idx) {
                                   payload[list_idx] = *cursor.payload();
                                   return payload;
                               });
        if (inter.empty()) {
            // Include only non-empty intersections.
            continue;
        }
        pair_mapping.emplace_back(left, right);
        while (not inter.empty()) {
            document_writer.push(*inter);
            payload_writer.push(inter.payload());
            inter.step();
        }
        document_writer.append(std::back_inserter(documents));
        payload_writer.append(std::back_inserter(payloads));
        document_offsets.push_back(documents.size());
        payload_offsets.push_back(payloads.size());
    }

    {
        // Hack to be backwards-compatible with binary_freq_collection (for now).
        auto one_bytes = std::array<std::byte, 4>{};
        auto size_bytes = std::array<std::byte, 4>{};
        auto num_bigrams = static_cast<std::uint32_t>(document_offsets.size());
        std::uint32_t one = 1;
        std::memcpy(&size_bytes, &num_bigrams, 4);
        std::memcpy(&one_bytes, &one, 4);
        std::copy(one_bytes.begin(), one_bytes.end(), documents.begin());
        std::copy(size_bytes.begin(), size_bytes.end(), std::next(documents.begin(), 4));
    }

    auto source = std::array<std::vector<std::byte>, 2>{std::move(documents), std::move(payloads)};
    auto document_span = gsl::make_span(source[0]);
    auto payload_span = gsl::make_span(source[1]);
    auto index =
        Index<RawReader<DocId>, RawReader<payload_type>, std::array<std::vector<std::byte>, 2>>(
            {},
            {},
            std::move(document_offsets),
            std::move(payload_offsets),
            document_span,
            payload_span,
            std::move(source));
    return BigramIndex(std::move(index), std::move(pair_mapping));
}

} // namespace pisa::v1
