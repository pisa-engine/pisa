#pragma once

#include <any>
#include <optional>

#include <gsl/span>

#include "index_metadata.hpp"

namespace pisa {

using DocId = std::uint32_t;
using TermId = std::uint32_t;

using OffsetSpan = gsl::span<std::size_t const>;
using BinarySpan = gsl::span<std::byte const>;

template <typename Cursor>
class Reader;

/// Note that offsets is stored here uncompressed.
/// For compressed, this needs to be slightly modified but the general idea is the same.
/// We'll need something that can get offsets for posting lists.
struct PostingData {
    BinarySpan postings;
    OffsetSpan offsets;
};

class Index {
   public:
    template <typename Source>
    Index(gsl::span<std::uint32_t const> document_lengths, float avg_document_length, Source source)
        : m_document_lengths(document_lengths),
          m_avg_document_length(avg_document_length),
          m_source(std::move(source))
    {
    }
    [[nodiscard]] auto num_terms() const -> std::size_t;
    [[nodiscard]] auto num_documents() const -> std::size_t;
    [[nodiscard]] auto document_length(DocId docid) const -> std::uint32_t;
    [[nodiscard]] auto avg_document_length() const -> float;
    [[nodiscard]] auto normalized_document_length(DocId docid) const -> float;

   protected:
    [[nodiscard]] auto fetch_postings(TermId term, PostingData const& data) const
        -> gsl::span<std::byte const>;

   private:
    gsl::span<std::uint32_t const> m_document_lengths;
    float m_avg_document_length;
    std::any m_source;
};

template <typename DocumentBlockEncoding>
class SaatCursor;

template <typename DocumentBlockEncoding>
class SaatIndex {
   public:
    SaatIndex() = default;
    [[nodiscard]] auto cursor(TermId term) const -> SaatCursor<DocumentBlockEncoding>
    {
        return m_posting_reader.read(this->fetch_postings(term, m_postings));
    }

   private:
    PostingData m_postings;
    Reader<SaatCursor<DocumentBlockEncoding>> m_posting_reader;
};

/// Your typical block-encoded postings.
template <typename BlockEncoding>
class BlockCursor;

/// Documents zipped with payloads like frequencies or scores.
template <typename DocumentCursor, typename PayloadCursor>
class DocumentPayloadCursor;

/// Cursor that scores frequency postings at query time.
template <typename Scorer>
class ScoringCursor;

enum class MaxScoreType { MaxScore, BlockMaxScore };

/// A "fake" scorer that will indicate to take quantized scores.
struct QuantizedScorer {
    config::Scorer scorer;
};

struct Bm25Scorer {
    // TODO: all the other stuff needed.
    config::Scorer scorer;
};

template <typename DocumentCursor, typename FrequencyCursor, typename ScoreCursor>
class DaatIndex {
   public:
    DaatIndex() = default;

    [[nodiscard]] auto documents(TermId term) const -> DocumentCursor
    {
        return m_document_reader.read(this->fetch_postings(term, m_documents));
    }

    [[nodiscard]] auto frequencies(TermId term) const -> FrequencyCursor
    {
        return m_frequency_reader.read(this->fetch_postings(term, m_frequencies));
    }

    [[nodiscard]] auto quantized_scores(config::Scorer scorer, TermId term) const -> ScoreCursor
    {
        return m_score_readers.at(scorer).read(this->fetch_postings(term, m_scores.at(scorer)));
    }

    [[nodiscard]] auto frequency_postings(TermId term) const;

    template <typename Scorer>
    [[nodiscard]] auto scored_postings(
        TermId term,
        Scorer scorer,
        std::optional<MaxScoreType> max_score_type = std::nullopt) const;

   private:
    PostingData m_documents;
    PostingData m_frequencies;
    std::map<config::Scorer, PostingData> m_scores;
    Reader<DocumentCursor> m_document_reader;
    Reader<FrequencyCursor> m_frequency_reader;
    std::map<config::Scorer, Reader<ScoreCursor>> m_score_readers;
};

} // namespace pisa
