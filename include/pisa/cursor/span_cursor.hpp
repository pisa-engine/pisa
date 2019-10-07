#pragma once

#include <cstdint>

#include <gsl/span>

namespace pisa {

/// This cursor operates on a span of contiguous memory.
/// Useful for using on intermediate results and testing.
struct SpanCursor {
    gsl::span<std::uint32_t const> documents;
    gsl::span<std::uint32_t const> frequencies;
    std::uint32_t max_docid;

    std::array<std::uint32_t, 1> sentinel_document;

    template <typename DocumentContainer, typename FrequencyContainer>
    constexpr SpanCursor(DocumentContainer const &documents,
                         FrequencyContainer const &frequencies,
                         std::uint32_t max_docid)
        : documents(documents), frequencies(frequencies), max_docid(max_docid)
    {
        sentinel_document[0] = max_docid;
    }

    [[nodiscard]] constexpr auto size() const noexcept -> std::size_t { return documents.size(); }
    [[nodiscard]] constexpr auto docid() const noexcept -> std::uint32_t { return documents[0]; }
    [[nodiscard]] constexpr auto freq() const noexcept -> float { return frequencies[0]; }
    constexpr void next()
    {
        if (documents[0] < max_docid) {
            documents = documents.subspan(1);
            frequencies = frequencies.subspan(1);
            try_finish();
        }
    }
    constexpr void next_geq(std::uint32_t docid)
    {
        if (documents[0] < max_docid) {
            auto new_pos = std::lower_bound(documents.begin(), documents.end(), docid);
            auto skip = std::distance(documents.begin(), new_pos);
            documents = documents.subspan(skip);
            frequencies = frequencies.subspan(skip);
            try_finish();
        }
    }

   private:
    constexpr void try_finish()
    {
        if (documents.empty()) {
            documents = gsl::make_span(sentinel_document);
        }
    }
};

} // namespace pisa
