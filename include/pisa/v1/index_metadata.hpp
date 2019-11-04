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
/// Otherwise, look for an `.ini` file in the current directory.
/// It will throw if no `.ini` file is found or there are multiple `.ini` files.
[[nodiscard]] auto resolve_ini(std::optional<std::string> const &arg) -> std::string;

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

struct IndexMetadata {
    PostingFilePaths documents;
    PostingFilePaths frequencies;
    std::string document_lengths_path;
    float avg_document_length;
    tl::optional<std::string> term_lexicon{};
    tl::optional<std::string> document_lexicon{};
    tl::optional<std::string> stemmer{};

    [[nodiscard]] static auto from_file(std::string const &file) -> IndexMetadata;
};

template <typename T>
[[nodiscard]] auto to_span(mio::mmap_source const *mmap)
{
    static_assert(std::is_trivially_constructible_v<T>);
    return gsl::span<T const>(reinterpret_cast<T const *>(mmap->data()), mmap->size() / sizeof(T));
};

template <typename T>
[[nodiscard]] auto source_span(MMapSource &source, std::string const &file)
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
    return IndexRunner<Readers...>(document_offsets,
                                   frequency_offsets,
                                   documents,
                                   frequencies,
                                   document_lengths,
                                   tl::make_optional(metadata.avg_document_length),
                                   std::move(source),
                                   std::move(readers));
}

} // namespace pisa::v1
