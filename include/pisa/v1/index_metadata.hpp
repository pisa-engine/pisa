#pragma once

#include <cstdint>
#include <functional>

#include <boost/property_tree/ini_parser.hpp>
#include <boost/property_tree/ptree.hpp>
#include <gsl/span>

#include "v1/index.hpp"
#include "v1/source.hpp"
#include "v1/types.hpp"

namespace pisa::v1 {

struct PostingFilePaths {
    std::string postings;
    std::string offsets;
};

struct IndexMetadata {
    PostingFilePaths documents;
    PostingFilePaths frequencies;
    std::string document_lengths_path;
    float avg_document_length;

    [[nodiscard]] static auto from_file(std::string const &file)
    {
        boost::property_tree::ptree pt;
        boost::property_tree::ini_parser::read_ini(file, pt);
        return IndexMetadata{
            .documents = PostingFilePaths{.postings = pt.get<std::string>("documents.file"),
                                          .offsets = pt.get<std::string>("documents.offsets")},
            .frequencies = PostingFilePaths{.postings = pt.get<std::string>("frequencies.file"),
                                            .offsets = pt.get<std::string>("frequencies.offsets")},
            .document_lengths_path = pt.get<std::string>("stats.document_lengths"),
            .avg_document_length = pt.get<float>("stats.avg_document_length")};
    }
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
                                   std::move(readers)...);
}

} // namespace pisa::v1
