#pragma once

#include <vector>

#include <fmt/format.h>
#include <gsl/span>

struct VectorCursor {
    gsl::span<std::uint32_t const> documents;
    gsl::span<std::uint32_t const> frequencies;
    std::uint32_t max_docid;

    std::array<std::uint32_t, 1> sentinel_document;

    [[nodiscard]] auto size() const noexcept -> std::size_t;
    [[nodiscard]] auto docid() const noexcept -> std::uint32_t;
    [[nodiscard]] auto freq() const noexcept -> float;
    void next();
    void next_geq(std::uint32_t docid);

  private:
    void try_finish();
};

struct InMemoryIndex {
    using document_enumerator = VectorCursor;

    std::vector<std::vector<std::uint32_t>> documents;
    std::vector<std::vector<std::uint32_t>> frequencies;
    std::uint32_t num_documents;

    [[nodiscard]] auto operator[](std::uint32_t term_id) const -> VectorCursor;
    [[nodiscard]] auto size() const noexcept -> std::size_t;
    [[nodiscard]] auto num_docs() const noexcept -> std::size_t;
};

struct InMemoryWand {
    std::vector<float> max_weights;
    std::uint32_t num_documents;

    [[nodiscard]] auto max_term_weight(std::uint32_t term_id) const noexcept -> float;
    [[nodiscard]] auto term_posting_count(std::uint32_t term_id) const noexcept -> std::uint32_t;
    [[nodiscard]] auto term_occurrence_count(std::uint32_t term_id) const noexcept -> std::uint32_t;

    [[nodiscard]] auto norm_len(std::uint32_t docid) const noexcept -> float;
    [[nodiscard]] auto doc_len(std::uint32_t docid) const noexcept -> std::uint32_t;
    [[nodiscard]] auto avg_len() const noexcept -> float;
    [[nodiscard]] auto num_docs() const noexcept -> std::size_t;
    [[nodiscard]] auto collection_len() const noexcept -> std::size_t;
};
