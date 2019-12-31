#pragma once

#include <any>
#include <array>
#include <string_view>

#include <gsl/span>
#include <tl/optional.hpp>

#include "v1/posting_builder.hpp"
#include "v1/source.hpp"
#include "v1/types.hpp"

namespace pisa::v1 {

using OffsetSpan = gsl::span<std::size_t const>;
using BinarySpan = gsl::span<std::byte const>;

[[nodiscard]] auto calc_avg_length(gsl::span<std::uint32_t const> const& lengths) -> float;
[[nodiscard]] auto read_sizes(std::string_view basename) -> std::vector<std::uint32_t>;

/// Lexicographically compares bigrams.
/// Used for looking up bigram mappings.
[[nodiscard]] auto compare_arrays(std::array<TermId, 2> const& lhs,
                                  std::array<TermId, 2> const& rhs) -> bool;

struct PostingData {
    BinarySpan postings;
    OffsetSpan offsets;
};

struct UnigramData {
    PostingData documents;
    PostingData payloads;
};

struct BigramData {
    PostingData documents;
    std::array<PostingData, 2> payloads;
    gsl::span<std::array<TermId, 2> const> mapping;
};

/// Parts of the index independent of the template parameters.
struct BaseIndex {

    template <typename Source>
    BaseIndex(PostingData documents,
              PostingData payloads,
              tl::optional<BigramData> bigrams,
              gsl::span<std::uint32_t const> document_lengths,
              tl::optional<float> avg_document_length,
              std::unordered_map<std::size_t, gsl::span<float const>> max_scores,
              std::unordered_map<std::size_t, UnigramData> block_max_scores,
              gsl::span<std::uint8_t const> quantized_max_scores,
              Source source)
        : m_documents(documents),
          m_payloads(payloads),
          m_bigrams(bigrams),
          m_document_lengths(document_lengths),
          m_avg_document_length(avg_document_length.map_or_else(
              [](auto&& self) { return self; },
              [&]() { return calc_avg_length(m_document_lengths); })),
          m_max_scores(std::move(max_scores)),
          m_block_max_scores(std::move(block_max_scores)),
          m_quantized_max_scores(quantized_max_scores),
          m_source(std::move(source))
    {
    }

    [[nodiscard]] auto num_terms() const -> std::size_t;
    [[nodiscard]] auto num_documents() const -> std::size_t;
    [[nodiscard]] auto document_length(DocId docid) const -> std::uint32_t;
    [[nodiscard]] auto avg_document_length() const -> float;
    [[nodiscard]] auto normalized_document_length(DocId docid) const -> float;
    [[nodiscard]] auto bigram_id(TermId left_term, TermId right_term) const -> tl::optional<TermId>;

   protected:
    void assert_term_in_bounds(TermId term) const;
    [[nodiscard]] auto fetch_documents(TermId term) const -> gsl::span<std::byte const>;
    [[nodiscard]] auto fetch_payloads(TermId term) const -> gsl::span<std::byte const>;
    [[nodiscard]] auto fetch_bigram_documents(TermId bigram) const -> gsl::span<std::byte const>;
    [[nodiscard]] auto fetch_bigram_payloads(TermId bigram) const
        -> std::array<gsl::span<std::byte const>, 2>;
    template <int Idx>
    [[nodiscard]] auto fetch_bigram_payloads(TermId bigram) const -> gsl::span<std::byte const>;

    [[nodiscard]] auto max_score(std::size_t scorer_hash, TermId term) const -> float;
    [[nodiscard]] auto block_max_scores(std::size_t scorer_hash) const -> UnigramData const&;
    [[nodiscard]] auto quantized_max_score(TermId term) const -> std::uint8_t;

   private:
    PostingData m_documents;
    PostingData m_payloads;
    tl::optional<BigramData> m_bigrams;

    gsl::span<std::uint32_t const> m_document_lengths;
    float m_avg_document_length;
    std::unordered_map<std::size_t, gsl::span<float const>> m_max_scores;
    std::unordered_map<std::size_t, UnigramData> m_block_max_scores;
    gsl::span<std::uint8_t const> m_quantized_max_scores;
    std::any m_source;
};

} // namespace pisa::v1
