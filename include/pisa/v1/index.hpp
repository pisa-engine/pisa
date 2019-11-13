#pragma once

#include <algorithm>
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
#include "v1/cursor/scoring_cursor.hpp"
#include "v1/cursor_intersection.hpp"
#include "v1/document_payload_cursor.hpp"
#include "v1/posting_builder.hpp"
#include "v1/raw_cursor.hpp"
#include "v1/scorer/bm25.hpp"
#include "v1/source.hpp"
#include "v1/types.hpp"

namespace pisa::v1 {

[[nodiscard]] inline auto calc_avg_length(gsl::span<std::uint32_t const> const &lengths) -> float
{
    auto sum = std::accumulate(lengths.begin(), lengths.end(), std::uint64_t(0), std::plus{});
    return static_cast<float>(sum) / lengths.size();
}

/// A generic type for an inverted index.
///
/// \tparam DocumentReader  Type of an object that reads document posting lists from bytes
///                         It must read lists containing `DocId` objects.
/// \tparam PayloadReader   Type of an object that reads payload posting lists from bytes.
///                         It can read lists of arbitrary types, such as `Frequency`,
///                         `Score`, or `std::pair<Score, Score>` for a bigram scored index.
template <typename DocumentCursor, typename PayloadCursor>
struct Index {

    using document_cursor_type = DocumentCursor;
    using payload_cursor_type = PayloadCursor;

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
          gsl::span<std::size_t const> document_offsets,
          gsl::span<std::size_t const> payload_offsets,
          gsl::span<std::byte const> documents,
          gsl::span<std::byte const> payloads,
          gsl::span<std::uint32_t const> document_lengths,
          tl::optional<float> avg_document_length,
          Source source)
        : m_document_reader(std::move(document_reader)),
          m_payload_reader(std::move(payload_reader)),
          m_document_offsets(document_offsets),
          m_payload_offsets(payload_offsets),
          m_documents(documents),
          m_payloads(payloads),
          m_document_lengths(document_lengths),
          m_avg_document_length(avg_document_length.map_or_else(
              [](auto &&self) { return self; },
              [&]() { return calc_avg_length(m_document_lengths); })),
          m_source(std::move(source))
    {
    }

    /// Constructs a new document-payload cursor (see document_payload_cursor.hpp).
    [[nodiscard]] auto cursor(TermId term) const
    {
        return DocumentPayloadCursor<DocumentCursor, PayloadCursor>(documents(term),
                                                                    payloads(term));
    }

    /// Constructs a new document-score cursor.
    template <typename Scorer>
    [[nodiscard]] auto scoring_cursor(TermId term, Scorer &&scorer) const
    {
        return ScoringCursor(cursor(term), std::forward<Scorer>(scorer).term_scorer(term));
    }

    /// This is equivalent to the `scoring_cursor` unless the scorer is of type `VoidScorer`,
    /// in which case index payloads are treated as scores.
    template <typename Scorer>
    [[nodiscard]] auto scored_cursor(TermId term, Scorer &&scorer) const
    {
        if constexpr (std::is_convertible_v<Scorer, VoidScorer>) {
            return cursor(term);
        } else {
            return scoring_cursor(term, std::forward<Scorer>(scorer));
        }
    }

    /// Constructs a new document cursor.
    [[nodiscard]] auto documents(TermId term) const
    {
        return m_document_reader.read(fetch_documents(term));
    }

    /// Constructs a new payload cursor.
    [[nodiscard]] auto payloads(TermId term) const
    {
        return m_payload_reader.read(fetch_payloads(term));
    }

    /// Constructs a new payload cursor.
    [[nodiscard]] auto num_terms() const -> std::size_t { return m_document_offsets.size() - 1; }

    [[nodiscard]] auto num_documents() const -> std::size_t { return m_document_lengths.size(); }

    [[nodiscard]] auto term_posting_count(TermId term) const -> std::uint32_t
    {
        // TODO(michal): Should be done more efficiently.
        return documents(term).size();
    }

    [[nodiscard]] auto document_length(DocId docid) const -> std::uint32_t
    {
        return m_document_lengths[docid];
    }

    [[nodiscard]] auto avg_document_length() const -> float { return m_avg_document_length; }

    [[nodiscard]] auto normalized_document_length(DocId docid) const -> float
    {
        return document_length(docid) / avg_document_length();
    }

   private:
    [[nodiscard]] auto fetch_documents(TermId term) const -> gsl::span<std::byte const>
    {
        Expects(term + 1 < m_document_offsets.size());
        return m_documents.subspan(m_document_offsets[term],
                                   m_document_offsets[term + 1] - m_document_offsets[term]);
    }
    [[nodiscard]] auto fetch_payloads(TermId term) const -> gsl::span<std::byte const>
    {
        Expects(term + 1 < m_payload_offsets.size());
        return m_payloads.subspan(m_payload_offsets[term],
                                  m_payload_offsets[term + 1] - m_payload_offsets[term]);
    }

    Reader<DocumentCursor> m_document_reader;
    Reader<PayloadCursor> m_payload_reader;
    gsl::span<std::size_t const> m_document_offsets;
    gsl::span<std::size_t const> m_payload_offsets;
    gsl::span<std::byte const> m_documents;
    gsl::span<std::byte const> m_payloads;
    gsl::span<std::uint32_t const> m_document_lengths;
    float m_avg_document_length;
    std::any m_source;
};

template <typename DocumentReader, typename PayloadReader, typename Source>
auto make_index(DocumentReader document_reader,
                PayloadReader payload_reader,
                gsl::span<std::size_t const> document_offsets,
                gsl::span<std::size_t const> payload_offsets,
                gsl::span<std::byte const> documents,
                gsl::span<std::byte const> payloads,
                gsl::span<std::uint32_t const> document_lengths,
                tl::optional<float> avg_document_length,
                Source source)
{
    using DocumentCursor =
        decltype(document_reader.read(std::declval<gsl::span<std::byte const>>()));
    using PayloadCursor = decltype(payload_reader.read(std::declval<gsl::span<std::byte const>>()));
    return Index<DocumentCursor, PayloadCursor>(std::move(document_reader),
                                                std::move(payload_reader),
                                                document_offsets,
                                                payload_offsets,
                                                documents,
                                                payloads,
                                                document_lengths,
                                                avg_document_length,
                                                std::move(source));
}

template <typename CharT,
          typename Index,
          typename Writer,
          typename Scorer,
          typename Quantizer,
          typename Callback>
auto score_index(Index const &index,
                 std::basic_ostream<CharT> &os,
                 Writer writer,
                 Scorer scorer,
                 Quantizer &&quantizer,
                 Callback &&callback) -> std::vector<std::size_t>
{
    PostingBuilder<typename Writer::value_type> score_builder(writer);
    score_builder.write_header(os);
    std::for_each(boost::counting_iterator<TermId>(0),
                  boost::counting_iterator<TermId>(index.num_terms()),
                  [&](auto term) {
                      for_each(index.scoring_cursor(term, scorer), [&](auto &cursor) {
                          score_builder.accumulate(quantizer(cursor.payload()));
                      });
                      score_builder.flush_segment(os);
                      callback();
                  });
    return std::move(score_builder.offsets());
}

template <typename CharT, typename Index, typename Writer, typename Scorer>
auto score_index(Index const &index, std::basic_ostream<CharT> &os, Writer writer, Scorer scorer)
    -> std::vector<std::size_t>
{
    PostingBuilder<typename Writer::value_type> score_builder(writer);
    score_builder.write_header(os);
    std::for_each(boost::counting_iterator<TermId>(0),
                  boost::counting_iterator<TermId>(index.num_terms()),
                  [&](auto term) {
                      for_each(index.scoring_cursor(term, scorer),
                               [&](auto &cursor) { score_builder.accumulate(cursor.payload()); });
                      score_builder.flush_segment(os);
                  });
    return std::move(score_builder.offsets());
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

inline auto read_sizes(std::string_view basename)
{
    binary_collection sizes(fmt::format("{}.sizes", basename).c_str());
    auto sequence = *sizes.begin();
    return std::vector<std::uint32_t>(sequence.begin(), sequence.end());
}

[[nodiscard]] inline auto binary_collection_source(std::string const &basename)
{
    using sink_type = boost::iostreams::back_insert_device<std::vector<std::byte>>;
    using vector_stream_type = boost::iostreams::stream<sink_type>;

    binary_freq_collection collection(basename.c_str());
    VectorSource source{{{}, {}}, {{}, {}}, {read_sizes(basename)}};
    std::vector<std::byte> &docbuf = source.bytes[0];
    std::vector<std::byte> &freqbuf = source.bytes[1];

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

    source.offsets[0] = std::move(document_builder.offsets());
    source.offsets[1] = std::move(frequency_builder.offsets());

    return source;
}

[[nodiscard]] inline auto binary_collection_index(std::string const &basename)
{
    auto source = binary_collection_source(basename);
    auto documents = gsl::span<std::byte const>(source.bytes[0]);
    auto frequencies = gsl::span<std::byte const>(source.bytes[1]);
    auto document_offsets = gsl::span<std::size_t const>(source.offsets[0]);
    auto frequency_offsets = gsl::span<std::size_t const>(source.offsets[1]);
    auto sizes = gsl::span<std::uint32_t const>(source.sizes[0]);
    return Index<RawCursor<DocId>, RawCursor<Frequency>>(RawReader<DocId>{},
                                                         RawReader<Frequency>{},
                                                         document_offsets,
                                                         frequency_offsets,
                                                         documents.subspan(8),
                                                         frequencies.subspan(8),
                                                         sizes,
                                                         tl::nullopt,
                                                         std::move(source));
}

[[nodiscard]] inline auto binary_collection_scored_index(std::string const &basename)
{
    using sink_type = boost::iostreams::back_insert_device<std::vector<std::byte>>;
    using vector_stream_type = boost::iostreams::stream<sink_type>;

    auto source = binary_collection_source(basename);
    auto documents = gsl::span<std::byte const>(source.bytes[0]);
    auto frequencies = gsl::span<std::byte const>(source.bytes[1]);
    auto sizes = gsl::span<std::uint32_t const>(source.sizes[0]);
    auto document_offsets = gsl::span<std::size_t>(source.offsets[0]);
    auto frequency_offsets = gsl::span<std::size_t>(source.offsets[1]);
    auto freq_index = Index<RawCursor<DocId>, RawCursor<Frequency>>(RawReader<DocId>{},
                                                                    RawReader<Frequency>{},
                                                                    document_offsets,
                                                                    frequency_offsets,
                                                                    documents.subspan(8),
                                                                    frequencies.subspan(8),
                                                                    sizes,
                                                                    tl::nullopt,
                                                                    false);

    source.offsets.push_back([&freq_index, &source]() {
        vector_stream_type score_stream{sink_type{source.bytes.emplace_back()}};
        return score_index(freq_index, score_stream, RawWriter<float>{}, make_bm25(freq_index));
    }());
    auto scores = gsl::span<std::byte const>(source.bytes.back());

    document_offsets = gsl::span<std::size_t>(source.offsets[0]);
    auto score_offsets = gsl::span<std::size_t>(source.offsets[2]);
    return Index<RawCursor<DocId>, RawCursor<float>>(RawReader<DocId>{},
                                                     RawReader<float>{},
                                                     document_offsets,
                                                     score_offsets,
                                                     documents.subspan(8),
                                                     scores.subspan(8),
                                                     sizes,
                                                     tl::nullopt,
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
    using sink_type = boost::iostreams::back_insert_device<std::vector<std::byte>>;
    using vector_stream_type = boost::iostreams::stream<sink_type>;

    auto unigram_index = binary_collection_index(basename);

    std::vector<std::pair<TermId, TermId>> pair_mapping;
    std::vector<std::byte> docbuf;
    std::vector<std::byte> freqbuf;

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

    VectorSource source{
        {std::move(docbuf), std::move(freqbuf)},
        {std::move(document_builder.offsets()), std::move(frequency_builder.offsets())},
        {read_sizes(basename)}};
    auto document_span = gsl::span<std::byte const>(source.bytes[0]);
    auto payload_span = gsl::span<std::byte const>(source.bytes[1]);
    auto document_offsets = gsl::span<std::size_t const>(source.offsets[0]);
    auto frequency_offsets = gsl::span<std::size_t const>(source.offsets[1]);
    auto sizes = gsl::span<std::uint32_t const>(source.sizes[0]);
    auto index = Index<RawCursor<DocId>, RawCursor<payload_type>>(RawReader<DocId>{},
                                                                  RawReader<payload_type>{},
                                                                  document_offsets,
                                                                  frequency_offsets,
                                                                  document_span.subspan(8),
                                                                  payload_span.subspan(8),
                                                                  sizes,
                                                                  tl::nullopt,
                                                                  std::move(source));
    return BigramIndex(std::move(index), std::move(pair_mapping));
}

template <typename... Readers>
struct IndexRunner {
    template <typename Source>
    IndexRunner(gsl::span<std::size_t const> document_offsets,
                gsl::span<std::size_t const> payload_offsets,
                gsl::span<std::byte const> documents,
                gsl::span<std::byte const> payloads,
                gsl::span<std::uint32_t const> document_lengths,
                tl::optional<float> avg_document_length,
                Source source,
                Readers... readers)
        : m_document_offsets(document_offsets),
          m_payload_offsets(payload_offsets),
          m_documents(documents),
          m_payloads(payloads),
          m_document_lengths(document_lengths),
          m_avg_document_length(avg_document_length),
          m_source(std::move(source)),
          m_readers(readers...)
    {
    }
    template <typename Source>
    IndexRunner(gsl::span<std::size_t const> document_offsets,
                gsl::span<std::size_t const> payload_offsets,
                gsl::span<std::byte const> documents,
                gsl::span<std::byte const> payloads,
                gsl::span<std::uint32_t const> document_lengths,
                tl::optional<float> avg_document_length,
                Source source,
                std::tuple<Readers...> readers)
        : m_document_offsets(document_offsets),
          m_payload_offsets(payload_offsets),
          m_documents(documents),
          m_payloads(payloads),
          m_document_lengths(document_lengths),
          m_avg_document_length(avg_document_length),
          m_source(std::move(source)),
          m_readers(std::move(readers))
    {
    }

    template <typename Fn>
    auto operator()(Fn fn)
    {
        auto dheader = PostingFormatHeader::parse(m_documents.first(8));
        auto pheader = PostingFormatHeader::parse(m_payloads.first(8));
        auto run = [&](auto &&dreader, auto &&preader) {
            if (std::decay_t<decltype(dreader)>::encoding() == dheader.encoding
                && std::decay_t<decltype(preader)>::encoding() == pheader.encoding
                && is_type<typename std::decay_t<decltype(dreader)>::value_type>(dheader.type)
                && is_type<typename std::decay_t<decltype(preader)>::value_type>(pheader.type)) {
                auto index = make_index(std::forward<decltype(dreader)>(dreader),
                                        std::forward<decltype(preader)>(preader),
                                        m_document_offsets,
                                        m_payload_offsets,
                                        m_documents.subspan(8),
                                        m_payloads.subspan(8),
                                        m_document_lengths,
                                        m_avg_document_length,
                                        false);
                fn(index);
                return true;
            }
            return false;
        };
        auto result = std::apply(
            [&](Readers... dreaders) {
                auto with_document_reader = [&](auto dreader) {
                    return std::apply(
                        [&](Readers... preaders) { return (run(dreader, preaders) || ...); },
                        m_readers);
                };
                return (with_document_reader(dreaders) || ...);
            },
            m_readers);
        if (not result) {
            throw std::domain_error("Unknown posting encoding");
        }
    }

   private:
    gsl::span<std::size_t const> m_document_offsets;
    gsl::span<std::size_t const> m_payload_offsets;
    gsl::span<std::byte const> m_documents;
    gsl::span<std::byte const> m_payloads;
    gsl::span<std::uint32_t const> m_document_lengths;
    tl::optional<float> m_avg_document_length;
    std::any m_source;
    std::tuple<Readers...> m_readers;
};

} // namespace pisa::v1
