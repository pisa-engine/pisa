#include <numeric>

#include <gsl/gsl_assert>

#include "binary_collection.hpp"
#include "v1/index.hpp"

namespace pisa::v1 {

[[nodiscard]] auto calc_avg_length(gsl::span<std::uint32_t const> const& lengths) -> float
{
    auto sum = std::accumulate(lengths.begin(), lengths.end(), std::uint64_t(0), std::plus{});
    return static_cast<float>(sum) / lengths.size();
}

[[nodiscard]] auto compare_arrays(std::array<TermId, 2> const& lhs,
                                  std::array<TermId, 2> const& rhs) -> bool
{
    if (std::get<0>(lhs) < std::get<0>(rhs)) {
        return true;
    }
    if (std::get<0>(lhs) > std::get<0>(rhs)) {
        return false;
    }
    return std::get<1>(lhs) < std::get<1>(rhs);
}

[[nodiscard]] auto read_sizes(std::string_view basename) -> std::vector<std::uint32_t>
{
    binary_collection sizes(fmt::format("{}.sizes", basename).c_str());
    auto sequence = *sizes.begin();
    return std::vector<std::uint32_t>(sequence.begin(), sequence.end());
}

[[nodiscard]] auto BaseIndex::num_terms() const -> std::size_t
{
    return m_documents.offsets.size() - 1;
}

[[nodiscard]] auto BaseIndex::num_documents() const -> std::size_t
{
    return m_document_lengths.size();
}

[[nodiscard]] auto BaseIndex::num_pairs() const -> std::size_t
{
    if (not m_bigrams) {
        throw std::logic_error("Bigrams are missing");
    }
    return m_bigrams->mapping.size();
}

[[nodiscard]] auto BaseIndex::document_length(DocId docid) const -> std::uint32_t
{
    return m_document_lengths[docid];
}

[[nodiscard]] auto BaseIndex::avg_document_length() const -> float { return m_avg_document_length; }

[[nodiscard]] auto BaseIndex::normalized_document_length(DocId docid) const -> float
{
    return document_length(docid) / avg_document_length();
}

void BaseIndex::assert_term_in_bounds(TermId term) const
{
    if (term >= num_terms()) {
        throw std::invalid_argument(
            fmt::format("Requested term ID out of bounds [0-{}): {}", num_terms(), term));
    }
}
[[nodiscard]] auto BaseIndex::fetch_documents(TermId term) const -> gsl::span<std::byte const>
{
    Expects(term + 1 < m_documents.offsets.size());
    return m_documents.postings.subspan(m_documents.offsets[term],
                                        m_documents.offsets[term + 1] - m_documents.offsets[term]);
}
[[nodiscard]] auto BaseIndex::fetch_payloads(TermId term) const -> gsl::span<std::byte const>
{
    Expects(term + 1 < m_payloads.offsets.size());
    return m_payloads.postings.subspan(m_payloads.offsets[term],
                                       m_payloads.offsets[term + 1] - m_payloads.offsets[term]);
}
[[nodiscard]] auto BaseIndex::fetch_bigram_documents(TermId bigram) const
    -> gsl::span<std::byte const>
{
    if (not m_bigrams) {
        throw std::logic_error("Bigrams are missing");
    }
    Expects(bigram + 1 < m_bigrams->documents.offsets.size());
    return m_bigrams->documents.postings.subspan(
        m_bigrams->documents.offsets[bigram],
        m_bigrams->documents.offsets[bigram + 1] - m_bigrams->documents.offsets[bigram]);
}

template <int Idx>
[[nodiscard]] auto BaseIndex::fetch_bigram_payloads(TermId bigram) const
    -> gsl::span<std::byte const>
{
    if (not m_bigrams) {
        throw std::logic_error("Bigrams are missing");
    }
    Expects(bigram + 1 < std::get<Idx>(m_bigrams->payloads).offsets.size());
    return std::get<Idx>(m_bigrams->payloads)
        .postings.subspan(std::get<Idx>(m_bigrams->payloads).offsets[bigram],
                          std::get<Idx>(m_bigrams->payloads).offsets[bigram + 1]
                              - std::get<Idx>(m_bigrams->payloads).offsets[bigram]);
}

template auto BaseIndex::fetch_bigram_payloads<0>(TermId bigram) const
    -> gsl::span<std::byte const>;
template auto BaseIndex::fetch_bigram_payloads<1>(TermId bigram) const
    -> gsl::span<std::byte const>;

[[nodiscard]] auto BaseIndex::fetch_bigram_payloads(TermId bigram) const
    -> std::array<gsl::span<std::byte const>, 2>
{
    return {fetch_bigram_payloads<0>(bigram), fetch_bigram_payloads<1>(bigram)};
}

[[nodiscard]] auto BaseIndex::bigram_id(TermId left_term, TermId right_term) const
    -> tl::optional<TermId>
{
    if (not m_bigrams) {
        throw std::logic_error("Bigrams are missing");
    }
    if (right_term == left_term) {
        throw std::logic_error("Requested bigram of two identical terms");
    }
    auto bigram = std::array<TermId, 2>{left_term, right_term};
    if (right_term < left_term) {
        std::swap(bigram[0], bigram[1]);
    }
    if (auto pos = std::lower_bound(
            m_bigrams->mapping.begin(), m_bigrams->mapping.end(), bigram, compare_arrays);
        pos != m_bigrams->mapping.end()) {
        if (*pos == bigram) {
            return tl::make_optional(std::distance(m_bigrams->mapping.begin(), pos));
        }
    }
    return tl::nullopt;
}

[[nodiscard]] auto BaseIndex::max_score(std::size_t scorer_hash, TermId term) const -> float
{
    if (m_max_scores.empty()) {
        throw std::logic_error("Missing max scores.");
    }
    return m_max_scores.at(scorer_hash)[term];
}

[[nodiscard]] auto BaseIndex::block_max_scores(std::size_t scorer_hash) const -> UnigramData const&
{
    if (auto pos = m_block_max_scores.find(scorer_hash); pos != m_block_max_scores.end()) {
        return pos->second;
    }
    throw std::logic_error("Missing block-max scores.");
}

[[nodiscard]] auto BaseIndex::quantized_max_score(TermId term) const -> std::uint8_t
{
    if (m_quantized_max_scores.empty()) {
        throw std::logic_error("Missing quantized max scores.");
    }
    return m_quantized_max_scores.at(term);
}

[[nodiscard]] auto BaseIndex::pairs() const -> tl::optional<gsl::span<std::array<TermId, 2> const>>
{
    return m_bigrams.map([](auto&& bigrams) { return bigrams.mapping; });
}

} // namespace pisa::v1
