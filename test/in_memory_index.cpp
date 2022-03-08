#include "in_memory_index.hpp"

auto VectorCursor::size() const noexcept -> std::size_t
{
    return documents.size();
}

auto VectorCursor::docid() const noexcept -> std::uint32_t
{
    return documents[0];
}

auto VectorCursor::freq() const noexcept -> float
{
    return frequencies[0];
}

void VectorCursor::next()
{
    if (documents[0] < max_docid) {
        documents = documents.subspan(1);
        frequencies = frequencies.subspan(1);
        try_finish();
    }
}
void VectorCursor::next_geq(std::uint32_t docid)
{
    if (documents[0] < max_docid) {
        auto new_pos = std::lower_bound(documents.begin(), documents.end(), docid);
        auto skip = std::distance(documents.begin(), new_pos);
        documents = documents.subspan(skip);
        frequencies = frequencies.subspan(skip);
        try_finish();
    }
}

void VectorCursor::try_finish()
{
    if (documents.empty()) {
        documents = gsl::make_span(sentinel_document);
    }
}

auto InMemoryIndex::operator[](std::uint32_t term_id) const -> VectorCursor
{
    if (term_id >= size()) {
        throw std::out_of_range(
            fmt::format("Term {} is out of range; index contains {} terms", term_id, size()));
    }
    return {gsl::make_span(documents[term_id]),
            gsl::make_span(frequencies[term_id]),
            num_documents,
            {num_documents}};
}

auto InMemoryIndex::size() const noexcept -> std::size_t
{
    return documents.size();
}

auto InMemoryIndex::num_docs() const noexcept -> std::size_t
{
    return num_documents;
}

auto InMemoryWand::max_term_weight(std::uint32_t term_id) const noexcept -> float
{
    return max_weights[term_id];
}

auto InMemoryWand::term_posting_count(std::uint32_t term_id) const noexcept -> std::uint32_t
{
    return 1;
}

auto InMemoryWand::term_occurrence_count(std::uint32_t term_id) const noexcept -> std::uint32_t
{
    return 1;
}

auto InMemoryWand::norm_len(std::uint32_t docid) const noexcept -> float
{
    return 1.0;
}

auto InMemoryWand::doc_len(std::uint32_t docid) const noexcept -> std::uint32_t
{
    return 1;
}

auto InMemoryWand::avg_len() const noexcept -> float
{
    return 1.0;
}

auto InMemoryWand::num_docs() const noexcept -> std::size_t
{
    return num_documents;
}

auto InMemoryWand::collection_len() const noexcept -> std::size_t
{
    return 1;
}
