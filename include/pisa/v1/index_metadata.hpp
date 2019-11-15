#pragma once

#include <cstdint>
#include <functional>

#include <gsl/span>
#include <tl/optional.hpp>

#include "v1/index.hpp"
#include "v1/source.hpp"
#include "v1/types.hpp"

namespace pisa::v1 {

/// Return the passed file path if is not `nullopt`.
/// Otherwise, look for an `.yml` file in the current directory.
/// It will throw if no `.yml` file is found or there are multiple `.yml` files.
[[nodiscard]] auto resolve_yml(std::optional<std::string> const& arg) -> std::string;

template <typename Optional>
[[nodiscard]] auto convert_optional(Optional opt)
{
    if (opt) {
        return tl::make_optional(*opt);
    }
    return tl::optional<std::decay_t<decltype(*opt)>>();
}

template <typename T>
[[nodiscard]] auto to_std(tl::optional<T> opt) -> std::optional<T>
{
    if (opt) {
        return std::make_optional(opt.take());
    }
    return std::optional<std::decay_t<decltype(*opt)>>();
}

struct PostingFilePaths {
    std::string postings;
    std::string offsets;
};

struct BigramMetadata {
    PostingFilePaths documents;
    std::pair<PostingFilePaths, PostingFilePaths> frequencies;
    std::string mapping;
    std::size_t count;
};

struct IndexMetadata {
    PostingFilePaths documents;
    PostingFilePaths frequencies;
    std::vector<PostingFilePaths> scores{};
    std::string document_lengths_path;
    float avg_document_length;
    tl::optional<std::string> term_lexicon{};
    tl::optional<std::string> document_lexicon{};
    tl::optional<std::string> stemmer{};
    tl::optional<BigramMetadata> bigrams{};

    void write(std::string const& file);
    [[nodiscard]] static auto from_file(std::string const& file) -> IndexMetadata;
};

template <typename T>
[[nodiscard]] auto to_span(mio::mmap_source const* mmap)
{
    static_assert(std::is_trivially_constructible_v<T>);
    return gsl::span<T const>(reinterpret_cast<T const*>(mmap->data()), mmap->size() / sizeof(T));
};

template <typename T>
[[nodiscard]] auto source_span(MMapSource& source, std::string const& file)
{
    return to_span<T>(
        source.file_sources.emplace_back(std::make_shared<mio::mmap_source>(file)).get());
};

template <typename... Readers>
[[nodiscard]] inline auto index_runner(IndexMetadata metadata, Readers... readers)
{
    return index_runner(std::move(metadata), std::make_tuple(readers...));
}

template <typename... Readers>
[[nodiscard]] inline auto index_runner(IndexMetadata metadata, std::tuple<Readers...> readers)
{
    MMapSource source;
    auto documents = source_span<std::byte>(source, metadata.documents.postings);
    auto frequencies = source_span<std::byte>(source, metadata.frequencies.postings);
    auto document_offsets = source_span<std::size_t>(source, metadata.documents.offsets);
    auto frequency_offsets = source_span<std::size_t>(source, metadata.frequencies.offsets);
    auto document_lengths = source_span<std::uint32_t>(source, metadata.document_lengths_path);
    tl::optional<gsl::span<std::size_t const>> bigram_document_offsets{};
    tl::optional<std::array<gsl::span<std::size_t const>, 2>> bigram_frequency_offsets{};
    tl::optional<gsl::span<std::byte const>> bigram_documents{};
    tl::optional<std::array<gsl::span<std::byte const>, 2>> bigram_frequencies{};
    tl::optional<::pisa::Payload_Vector<std::array<std::size_t, 2>>> bigram_mapping{};
    if (metadata.bigrams) {
        bigram_document_offsets =
            source_span<std::size_t>(source, metadata.bigrams->documents.offsets);
        bigram_frequency_offsets = {
            source_span<std::size_t>(source, metadata.bigrams->frequencies.first.offsets),
            source_span<std::size_t>(source, metadata.bigrams->frequencies.second.offsets)};
        bigram_documents = source_span<std::byte>(source, metadata.bigrams->documents.postings);
        bigram_frequencies = {
            source_span<std::byte>(source, metadata.bigrams->frequencies.first.postings),
            source_span<std::byte>(source, metadata.bigrams->frequencies.second.postings)};
        auto mapping_span = source_span<std::byte>(source, metadata.bigrams->mapping).subspan(8);
        auto num_offset_bytes = (metadata.bigrams->count + 1U) * 8U;
        auto mapping_offsets = mapping_span.first(num_offset_bytes);
        bigram_mapping = Payload_Vector<std::array<std::size_t, 2>>(
            gsl::span<std::size_t const>(
                reinterpret_cast<std::size_t const*>(mapping_offsets.data()),
                mapping_offsets.size() * sizeof(std::size_t)),
            mapping_span.subspan(num_offset_bytes));
    }
    return IndexRunner<Readers...>(document_offsets,
                                   frequency_offsets,
                                   bigram_document_offsets,
                                   bigram_frequency_offsets,
                                   documents,
                                   frequencies,
                                   bigram_documents,
                                   bigram_frequencies,
                                   document_lengths,
                                   tl::make_optional(metadata.avg_document_length),
                                   bigram_mapping,
                                   std::move(source),
                                   std::move(readers));
}

template <typename... Readers>
[[nodiscard]] inline auto scored_index_runner(IndexMetadata metadata, Readers... readers)
{
    return scored_index_runner(std::move(metadata), std::make_tuple(readers...));
}

template <typename... Readers>
[[nodiscard]] inline auto scored_index_runner(IndexMetadata metadata,
                                              std::tuple<Readers...> readers)
{
    MMapSource source;
    auto documents = source_span<std::byte>(source, metadata.documents.postings);
    auto scores = source_span<std::byte>(source, metadata.scores.front().postings);
    auto document_offsets = source_span<std::size_t>(source, metadata.documents.offsets);
    auto score_offsets = source_span<std::size_t>(source, metadata.scores.front().offsets);
    auto document_lengths = source_span<std::uint32_t>(source, metadata.document_lengths_path);
    return IndexRunner<Readers...>(document_offsets,
                                   score_offsets,
                                   {},
                                   {},
                                   documents,
                                   scores,
                                   {}, // TODO(michal): support scored bigrams
                                   {},
                                   document_lengths,
                                   tl::make_optional(metadata.avg_document_length),
                                   {}, // TODO
                                   std::move(source),
                                   std::move(readers));
}

} // namespace pisa::v1
