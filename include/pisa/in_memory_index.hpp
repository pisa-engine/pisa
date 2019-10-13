#pragma once

#include <fmt/format.h>
#include <gsl/span>

#include "cursor/span_cursor.hpp"

namespace pisa {

struct InMemoryIndex {
    using document_enumerator = SpanCursor;

    std::vector<std::vector<std::uint32_t>> documents;
    std::vector<std::vector<std::uint32_t>> frequencies;
    std::uint32_t num_documents;

    [[nodiscard]] auto operator[](std::uint32_t term_id) const -> SpanCursor
    {
        if (term_id >= size()) {
            throw std::out_of_range(
                fmt::format("Term {} is out of range; index contains {} terms", term_id, size()));
        }
        return SpanCursor(gsl::make_span(documents[term_id]),
                          gsl::make_span(frequencies[term_id]),
                          num_documents);
    }

    [[nodiscard]] auto size() const noexcept -> std::size_t { return documents.size(); }
    [[nodiscard]] auto num_docs() const noexcept -> std::size_t { return num_documents; }
};

} // namespace pisa

struct InMemoryWand {
    std::vector<float> max_weights;
    std::vector<float> term_posting_counts;
    std::uint32_t ndocs;

    template <typename Index>
    InMemoryWand(Index const &index)
    {
        for (std::uint32_t term_id = 0; term_id < index.size(); ++term_id) {
            term_posting_counts.push_back(index[term_id].size());
        }
    }

    [[nodiscard]] auto max_term_weight(std::uint32_t term_id) const noexcept -> float
    {
        return max_weights[term_id];
    }

    [[nodiscard]] auto norm_len(std::uint32_t docid) const noexcept { return 1.0; }
    [[nodiscard]] auto term_posting_count(std::uint32_t term_id) const noexcept
    {
        return term_posting_counts[term_id];
    }
    [[nodiscard]] auto num_docs() const noexcept -> std::uint32_t { return ndocs; }
};
