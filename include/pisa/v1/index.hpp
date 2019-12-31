#pragma once

#include <algorithm>
#include <any>

#include <fmt/format.h>
#include <gsl/span>
#include <tl/optional.hpp>

#include "v1/base_index.hpp"
#include "v1/bit_cast.hpp"
#include "v1/cursor/for_each.hpp"
#include "v1/cursor/scoring_cursor.hpp"
#include "v1/document_payload_cursor.hpp"
#include "v1/posting_builder.hpp"
#include "v1/raw_cursor.hpp"
#include "v1/source.hpp"
#include "v1/types.hpp"
#include "v1/zip_cursor.hpp"

namespace pisa::v1 {

/// A generic type for an inverted index.
///
/// \tparam DocumentReader  Type of an object that reads document posting lists from bytes
///                         It must read lists containing `DocId` objects.
/// \tparam PayloadReader   Type of an object that reads payload posting lists from bytes.
///                         It can read lists of arbitrary types, such as `Frequency`,
///                         `Score`, or `std::pair<Score, Score>` for a bigram scored index.
template <typename DocumentCursor, typename PayloadCursor>
struct Index : public BaseIndex {

    using document_cursor_type = DocumentCursor;
    using payload_cursor_type = PayloadCursor;

    /// Constructs the index.
    ///
    /// \param document_reader  Reads document posting lists from bytes.
    /// \param payload_reader   Reads payload posting lists from bytes.
    /// TODO(michal)...
    /// \param source           This object (optionally) owns the raw data pointed at by
    ///                         `documents` and `payloads` to ensure it is valid throughout
    ///                         the lifetime of the index. It should release any resources
    ///                         in its destructor.
    template <typename DocumentReader, typename PayloadReader, typename Source>
    Index(DocumentReader document_reader,
          PayloadReader payload_reader,
          PostingData documents,
          PostingData payloads,
          tl::optional<BigramData> bigrams,
          gsl::span<std::uint32_t const> document_lengths,
          tl::optional<float> avg_document_length,
          std::unordered_map<std::size_t, gsl::span<float const>> max_scores,
          std::unordered_map<std::size_t, UnigramData> block_max_scores,
          gsl::span<std::uint8_t const> quantized_max_scores,
          Source source)
        : BaseIndex(documents,
                    payloads,
                    bigrams,
                    document_lengths,
                    avg_document_length,
                    std::move(max_scores),
                    std::move(block_max_scores),
                    quantized_max_scores,
                    source),
          m_document_reader(std::move(document_reader)),
          m_payload_reader(std::move(payload_reader))
    {
    }

    /// Constructs a new document-payload cursor (see document_payload_cursor.hpp).
    [[nodiscard]] auto cursor(TermId term) const
    {
        return DocumentPayloadCursor<DocumentCursor, PayloadCursor>(documents(term),
                                                                    payloads(term));
    }

    [[nodiscard]] auto cursors(gsl::span<TermId const> terms) const
    {
        std::vector<decltype(cursor(0))> cursors;
        std::transform(terms.begin(), terms.end(), std::back_inserter(cursors), [&](auto term) {
            return cursor(term);
        });
        return cursors;
    }

    [[nodiscard]] auto bigram_payloads_0(TermId left_term, TermId right_term) const
    {
        return bigram_id(left_term, right_term).map([this](auto bid) {
            return m_payload_reader.read(fetch_bigram_payloads<0>(bid));
        });
    }

    [[nodiscard]] auto bigram_payloads_1(TermId left_term, TermId right_term) const
    {
        return bigram_id(left_term, right_term).map([this](auto bid) {
            return m_payload_reader.read(fetch_bigram_payloads<1>(bid));
        });
    }

    [[nodiscard]] auto bigram_cursor(TermId left_term, TermId right_term) const
    {
        return bigram_id(left_term, right_term).map([this](auto bid) {
            return document_payload_cursor(
                m_document_reader.read(fetch_bigram_documents(bid)),
                zip(m_payload_reader.read(fetch_bigram_payloads<0>(bid)),
                    m_payload_reader.read(fetch_bigram_payloads<1>(bid))));
        });
    }

    /// Constructs a new document-score cursor.
    template <typename Scorer>
    [[nodiscard]] auto scoring_cursor(TermId term, Scorer&& scorer) const
    {
        return ScoringCursor(cursor(term), std::forward<Scorer>(scorer).term_scorer(term));
    }

    template <typename Scorer>
    [[nodiscard]] auto scoring_cursors(gsl::span<TermId const> terms, Scorer&& scorer) const
    {
        std::vector<decltype(scoring_cursor(0, scorer))> cursors;
        std::transform(terms.begin(), terms.end(), std::back_inserter(cursors), [&](auto term) {
            return scoring_cursor(term, scorer);
        });
        return cursors;
    }

    /// This is equivalent to the `scoring_cursor` unless the scorer is of type `VoidScorer`,
    /// in which case index payloads are treated as scores.
    template <typename Scorer>
    [[nodiscard]] auto scored_cursor(TermId term, Scorer&& scorer) const
    {
        if constexpr (std::is_convertible_v<Scorer, VoidScorer>) {
            return cursor(term);
        } else {
            return scoring_cursor(term, std::forward<Scorer>(scorer));
        }
    }

    template <typename Scorer>
    [[nodiscard]] auto scored_cursors(gsl::span<TermId const> terms, Scorer&& scorer) const
    {
        std::vector<decltype(scored_cursor(0, scorer))> cursors;
        std::transform(terms.begin(), terms.end(), std::back_inserter(cursors), [&](auto term) {
            return scored_cursor(term, scorer);
        });
        return cursors;
    }

    template <typename Scorer>
    [[nodiscard]] auto max_scored_cursor(TermId term, Scorer&& scorer) const
    {
        using cursor_type =
            std::decay_t<decltype(scored_cursor(term, std::forward<Scorer>(scorer)))>;
        if constexpr (std::is_convertible_v<Scorer, VoidScorer>) {
            return MaxScoreCursor<cursor_type, float>(
                scored_cursor(term, std::forward<Scorer>(scorer)), quantized_max_score(term));
        } else {
            return MaxScoreCursor<cursor_type, float>(
                scored_cursor(term, std::forward<Scorer>(scorer)),
                max_score(std::hash<std::decay_t<Scorer>>{}(scorer), term));
        }
    }

    template <typename Scorer>
    [[nodiscard]] auto max_scored_cursors(gsl::span<TermId const> terms, Scorer&& scorer) const
    {
        std::vector<decltype(max_scored_cursor(0, scorer))> cursors;
        std::transform(terms.begin(), terms.end(), std::back_inserter(cursors), [&](auto term) {
            return max_scored_cursor(term, scorer);
        });
        return cursors;
    }

    template <typename Scorer>
    [[nodiscard]] auto block_max_scored_cursor(TermId term, Scorer&& scorer) const
    {
        auto const& document_reader = block_max_document_reader();
        auto const& score_reader = block_max_score_reader();
        // Expects(term + 1 < m_payloads.offsets.size());
        if constexpr (std::is_convertible_v<Scorer, VoidScorer>) {
            if (false) { // TODO(michal): Workaround for now to avoid explicitly defining return
                         // type.
                return block_max_score_cursor(
                    scored_cursor(term, std::forward<Scorer>(scorer)),
                    document_payload_cursor(document_reader.read({}), score_reader.read({})),
                    0.0F);
            }
            throw std::logic_error("Quantized block max scores uimplemented");
        } else {
            auto const& data = block_max_scores(std::hash<std::decay_t<Scorer>>{}(scorer));
            auto block_max_document_subspan = data.documents.postings.subspan(
                data.documents.offsets[term],
                data.documents.offsets[term + 1] - data.documents.offsets[term]);
            auto block_max_score_subspan = data.payloads.postings.subspan(
                data.payloads.offsets[term],
                data.payloads.offsets[term + 1] - data.payloads.offsets[term]);
            return block_max_score_cursor(
                scored_cursor(term, std::forward<Scorer>(scorer)),
                document_payload_cursor(document_reader.read(block_max_document_subspan),
                                        score_reader.read(block_max_score_subspan)),
                max_score(std::hash<std::decay_t<Scorer>>{}(scorer), term));
        }
    }

    template <typename Scorer>
    [[nodiscard]] auto block_max_scored_cursors(gsl::span<TermId const> terms,
                                                Scorer&& scorer) const
    {
        std::vector<decltype(block_max_scored_cursor<Scorer>(0, std::forward<Scorer>(scorer)))>
            cursors;
        std::transform(terms.begin(), terms.end(), std::back_inserter(cursors), [&](auto term) {
            return block_max_scored_cursor(term, scorer);
        });
        return cursors;
    }

    /// Constructs a new document-score cursor.
    template <typename Scorer>
    [[nodiscard]] auto scoring_bigram_cursor(TermId left_term,
                                             TermId right_term,
                                             Scorer&& scorer) const
    {
        return bigram_cursor(left_term, right_term)
            .take()
            .map([scorer = std::forward<Scorer>(scorer), left_term, right_term](auto cursor) {
                return ScoringCursor(cursor,
                                     [scorers = std::make_tuple(scorer.term_scorer(left_term),
                                                                scorer.term_scorer(right_term))](
                                         auto&& docid, auto&& payload) {
                                         return std::array<float, 2>{
                                             std::get<0>(scorers)(docid, std::get<0>(payload)),
                                             std::get<1>(scorers)(docid, std::get<1>(payload))};
                                     });
            });
    }

    template <typename Scorer>
    [[nodiscard]] auto scored_bigram_cursor(TermId left_term,
                                            TermId right_term,
                                            Scorer&& scorer) const
    {
        if constexpr (std::is_convertible_v<Scorer, VoidScorer>) {
            return bigram_cursor(left_term, right_term);
        } else {
            return scoring_bigram_cursor(left_term, right_term, std::forward<Scorer>(scorer));
        }
    }

    /// Constructs a new document cursor.
    [[nodiscard]] auto documents(TermId term) const
    {
        assert_term_in_bounds(term);
        return m_document_reader.read(fetch_documents(term));
    }

    /// Constructs a new payload cursor.
    [[nodiscard]] auto payloads(TermId term) const
    {
        assert_term_in_bounds(term);
        return m_payload_reader.read(fetch_payloads(term));
    }

    [[nodiscard]] auto term_posting_count(TermId term) const -> std::uint32_t
    {
        // TODO(michal): Should be done more efficiently.
        return documents(term).size();
    }

    [[nodiscard]] auto block_max_document_reader() const -> Reader<RawCursor<DocId>> const&
    {
        return m_block_max_document_reader;
    }

    [[nodiscard]] auto block_max_score_reader() const -> Reader<RawCursor<float>> const&
    {
        return m_block_max_score_reader;
    }

   private:
    Reader<DocumentCursor> m_document_reader;
    Reader<PayloadCursor> m_payload_reader;

    Reader<RawCursor<DocId>> m_block_max_document_reader =
        Reader<RawCursor<DocId>>(RawReader<DocId>{});
    Reader<RawCursor<float>> m_block_max_score_reader =
        Reader<RawCursor<float>>(RawReader<float>{});
};

template <typename DocumentReader, typename PayloadReader, typename Source>
auto make_index(DocumentReader document_reader,
                PayloadReader payload_reader,
                PostingData documents,
                PostingData payloads,
                tl::optional<BigramData> bigrams,
                gsl::span<std::uint32_t const> document_lengths,
                tl::optional<float> avg_document_length,
                std::unordered_map<std::size_t, gsl::span<float const>> max_scores,
                std::unordered_map<std::size_t, UnigramData> block_max_scores,
                gsl::span<std::uint8_t const> quantized_max_scores,
                Source source)
{
    using DocumentCursor =
        decltype(document_reader.read(std::declval<gsl::span<std::byte const>>()));
    using PayloadCursor = decltype(payload_reader.read(std::declval<gsl::span<std::byte const>>()));
    return Index<DocumentCursor, PayloadCursor>(std::move(document_reader),
                                                std::move(payload_reader),
                                                documents,
                                                payloads,
                                                bigrams,
                                                document_lengths,
                                                avg_document_length,
                                                std::move(max_scores),
                                                std::move(block_max_scores),
                                                quantized_max_scores,
                                                std::move(source));
}

template <typename DocumentReaders, typename PayloadReaders>
struct IndexRunner {
    template <typename Source>
    IndexRunner(PostingData documents,
                PostingData payloads,
                tl::optional<BigramData> bigrams,
                gsl::span<std::uint32_t const> document_lengths,
                tl::optional<float> avg_document_length,
                std::unordered_map<std::size_t, gsl::span<float const>> max_scores,
                std::unordered_map<std::size_t, UnigramData> block_max_scores,
                gsl::span<std::uint8_t const> quantized_max_scores,
                Source source,
                DocumentReaders document_readers,
                PayloadReaders payload_readers)
        : m_documents(documents),
          m_payloads(payloads),
          m_bigrams(bigrams),
          m_document_lengths(document_lengths),
          m_avg_document_length(avg_document_length),
          m_max_scores(std::move(max_scores)),
          m_block_max_scores(std::move(block_max_scores)),
          m_max_quantized_scores(quantized_max_scores),
          m_source(std::move(source)),
          m_document_readers(std::move(document_readers)),
          m_payload_readers(std::move(payload_readers))
    {
    }

    template <typename Fn>
    auto operator()(Fn fn)
    {
        auto dheader = PostingFormatHeader::parse(m_documents.postings.first(8));
        auto pheader = PostingFormatHeader::parse(m_payloads.postings.first(8));
        auto run = [&](auto&& dreader, auto&& preader) {
            if (std::decay_t<decltype(dreader)>::encoding() == dheader.encoding
                && std::decay_t<decltype(preader)>::encoding() == pheader.encoding
                && is_type<typename std::decay_t<decltype(dreader)>::value_type>(dheader.type)
                && is_type<typename std::decay_t<decltype(preader)>::value_type>(pheader.type)) {
                auto block_max_scores = m_block_max_scores;
                for (auto& [key, data] : block_max_scores) {
                    data.documents.postings = data.documents.postings.subspan(8);
                    data.payloads.postings = data.payloads.postings.subspan(8);
                }
                fn(make_index(
                    std::forward<decltype(dreader)>(dreader),
                    std::forward<decltype(preader)>(preader),
                    PostingData{.postings = m_documents.postings.subspan(8),
                                .offsets = m_documents.offsets},
                    PostingData{.postings = m_payloads.postings.subspan(8),
                                .offsets = m_payloads.offsets},
                    m_bigrams.map([](auto&& bigram_data) {
                        return BigramData{
                            .documents =
                                PostingData{.postings = bigram_data.documents.postings.subspan(8),
                                            .offsets = bigram_data.documents.offsets},
                            .payloads =
                                std::array<PostingData, 2>{
                                    PostingData{
                                        .postings =
                                            std::get<0>(bigram_data.payloads).postings.subspan(8),
                                        .offsets = std::get<0>(bigram_data.payloads).offsets},
                                    PostingData{
                                        .postings =
                                            std::get<1>(bigram_data.payloads).postings.subspan(8),
                                        .offsets = std::get<1>(bigram_data.payloads).offsets}},
                            .mapping = bigram_data.mapping};
                    }),
                    m_document_lengths,
                    m_avg_document_length,
                    m_max_scores,
                    block_max_scores,
                    m_max_quantized_scores,
                    false));
                return true;
            }
            return false;
        };
        auto result = std::apply(
            [&](auto... dreaders) {
                auto with_document_reader = [&](auto dreader) {
                    return std::apply(
                        [&](auto... preaders) { return (run(dreader, preaders) || ...); },
                        m_payload_readers);
                };
                return (with_document_reader(dreaders) || ...);
            },
            m_document_readers);
        if (not result) {
            std::ostringstream os;
            os << fmt::format(
                "Unknown posting encoding. Requested document: "
                "{:x} ({:b}), payload: {:x} ({:b})\n",
                dheader.encoding,
                static_cast<std::uint8_t>(to_byte(dheader.type)),
                pheader.encoding,
                static_cast<std::uint8_t>(to_byte(pheader.type)));
            auto print_reader = [&](auto&& reader) {
                os << fmt::format(
                    "\t{:x} ({:b})\n",
                    reader.encoding(),
                    static_cast<std::uint8_t>(to_byte(
                        value_type<typename std::decay_t<decltype(reader)>::value_type>())));
            };
            os << "Available document readers: \n";
            std::apply([&](auto... readers) { (print_reader(readers), ...); }, m_document_readers);
            os << "Available payload readers: \n";
            std::apply([&](auto... readers) { (print_reader(readers), ...); }, m_payload_readers);
            throw std::domain_error(os.str());
        }
    }

   private:
    PostingData m_documents;
    PostingData m_payloads;
    tl::optional<BigramData> m_bigrams;

    gsl::span<std::uint32_t const> m_document_lengths;
    tl::optional<float> m_avg_document_length;
    std::unordered_map<std::size_t, gsl::span<float const>> m_max_scores;
    std::unordered_map<std::size_t, UnigramData> m_block_max_scores;
    gsl::span<std::uint8_t const> m_max_quantized_scores;
    tl::optional<gsl::span<std::array<TermId, 2> const>> m_bigram_mapping;
    std::any m_source;
    DocumentReaders m_document_readers;
    PayloadReaders m_payload_readers;
};

} // namespace pisa::v1
