#pragma once

#include <cstdint>
#include <functional>

#include <gsl/span>
#include <tl/optional.hpp>
#include <yaml-cpp/yaml.h>

#include "v1/index.hpp"
#include "v1/query.hpp"
#include "v1/source.hpp"
#include "v1/types.hpp"

namespace pisa::v1 {

[[nodiscard]] auto append_extension(std::string file_path) -> std::string;

/// Return the passed file path if is not `nullopt`.
/// Otherwise, look for an `.yml` file in the current directory.
/// It will throw if no `.yml` file is found or there are multiple `.yml` files.
[[nodiscard]] auto resolve_yml(tl::optional<std::string> const& arg) -> std::string;

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

struct UnigramFilePaths {
    PostingFilePaths documents;
    PostingFilePaths payloads;
};

struct BigramMetadata {
    PostingFilePaths documents;
    std::pair<PostingFilePaths, PostingFilePaths> frequencies;
    std::vector<std::pair<PostingFilePaths, PostingFilePaths>> scores{};
    std::string mapping;
    std::size_t count;
};

struct IndexMetadata final {
    tl::optional<std::string> basename{};
    PostingFilePaths documents;
    PostingFilePaths frequencies;
    std::vector<PostingFilePaths> scores{};
    std::string document_lengths_path;
    float avg_document_length;
    tl::optional<std::string> term_lexicon{};
    tl::optional<std::string> document_lexicon{};
    tl::optional<std::string> stemmer{};
    tl::optional<BigramMetadata> bigrams{};
    std::map<std::string, std::string> max_scores{};
    std::map<std::string, UnigramFilePaths> block_max_scores{};
    std::map<std::string, std::string> quantized_max_scores{};

    void write(std::string const& file) const;
    void update() const;
    [[nodiscard]] auto query_parser(tl::optional<std::string> const& stop_words = tl::nullopt) const
        -> std::function<void(Query&)>;
    [[nodiscard]] auto get_basename() const -> std::string const&;
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

template <typename DocumentReaders, typename PayloadReaders>
[[nodiscard]] inline auto index_runner(IndexMetadata metadata,
                                       DocumentReaders document_readers,
                                       PayloadReaders payload_readers)
{
    MMapSource source;
    auto documents = source_span<std::byte>(source, metadata.documents.postings);
    auto frequencies = source_span<std::byte>(source, metadata.frequencies.postings);
    auto document_offsets = source_span<std::size_t>(source, metadata.documents.offsets);
    auto frequency_offsets = source_span<std::size_t>(source, metadata.frequencies.offsets);
    auto document_lengths = source_span<std::uint32_t>(source, metadata.document_lengths_path);
    auto bigrams = [&]() -> tl::optional<BigramData> {
        gsl::span<std::size_t const> bigram_document_offsets{};
        std::array<gsl::span<std::size_t const>, 2> bigram_frequency_offsets{};
        gsl::span<std::byte const> bigram_documents{};
        std::array<gsl::span<std::byte const>, 2> bigram_frequencies{};
        gsl::span<std::array<TermId, 2> const> bigram_mapping{};
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
            auto mapping_span = source_span<std::byte>(source, metadata.bigrams->mapping);
            bigram_mapping = gsl::span<std::array<TermId, 2> const>(
                reinterpret_cast<std::array<TermId, 2> const*>(mapping_span.data()),
                mapping_span.size() / (sizeof(TermId) * 2));
            return BigramData{
                .documents = {.postings = bigram_documents, .offsets = bigram_document_offsets},
                .payloads =
                    std::array<PostingData, 2>{
                        PostingData{.postings = std::get<0>(bigram_frequencies),
                                    .offsets = std::get<0>(bigram_frequency_offsets)},
                        PostingData{.postings = std::get<1>(bigram_frequencies),
                                    .offsets = std::get<1>(bigram_frequency_offsets)}},
                .mapping = bigram_mapping};
        }
        return tl::nullopt;
    }();
    std::unordered_map<std::size_t, gsl::span<float const>> max_scores;
    if (not metadata.max_scores.empty()) {
        for (auto [name, file] : metadata.max_scores) {
            auto bytes = source_span<std::byte>(source, file);
            max_scores[std::hash<std::string>{}(name)] = gsl::span<float const>(
                reinterpret_cast<float const*>(bytes.data()), bytes.size() / (sizeof(float)));
        }
    }
    std::unordered_map<std::size_t, UnigramData> block_max_scores;
    if (not metadata.block_max_scores.empty()) {
        for (auto [name, files] : metadata.block_max_scores) {
            auto document_bytes = source_span<std::byte>(source, files.documents.postings);
            auto document_offsets = source_span<std::size_t>(source, files.documents.offsets);
            auto payload_bytes = source_span<std::byte>(source, files.payloads.postings);
            auto payload_offsets = source_span<std::size_t>(source, files.payloads.offsets);
            block_max_scores[std::hash<std::string>{}(name)] =
                UnigramData{.documents = {.postings = document_bytes, .offsets = document_offsets},
                            .payloads = {.postings = payload_bytes, .offsets = payload_offsets}};
        }
    }
    return IndexRunner<DocumentReaders, PayloadReaders>(
        {.postings = documents, .offsets = document_offsets},
        {.postings = frequencies, .offsets = frequency_offsets},
        bigrams,
        document_lengths,
        tl::make_optional(metadata.avg_document_length),
        std::move(max_scores),
        std::move(block_max_scores),
        {},
        std::move(source),
        std::move(document_readers),
        std::move(payload_readers));
}

template <typename DocumentReaders, typename PayloadReaders>
[[nodiscard]] inline auto scored_index_runner(IndexMetadata metadata,
                                              DocumentReaders document_readers,
                                              PayloadReaders payload_readers)
{
    MMapSource source;
    auto documents = source_span<std::byte>(source, metadata.documents.postings);
    // TODO(michal): support many precomputed scores
    auto scores = source_span<std::byte>(source, metadata.scores.front().postings);
    auto document_offsets = source_span<std::size_t>(source, metadata.documents.offsets);
    auto score_offsets = source_span<std::size_t>(source, metadata.scores.front().offsets);
    auto document_lengths = source_span<std::uint32_t>(source, metadata.document_lengths_path);
    auto bigrams = [&]() -> tl::optional<BigramData> {
        gsl::span<std::size_t const> bigram_document_offsets{};
        std::array<gsl::span<std::size_t const>, 2> bigram_score_offsets{};
        gsl::span<std::byte const> bigram_documents{};
        std::array<gsl::span<std::byte const>, 2> bigram_scores{};
        gsl::span<std::array<TermId, 2> const> bigram_mapping{};
        if (metadata.bigrams && not metadata.bigrams->scores.empty()) {
            bigram_document_offsets =
                source_span<std::size_t>(source, metadata.bigrams->documents.offsets);
            bigram_score_offsets = {
                source_span<std::size_t>(source, metadata.bigrams->scores[0].first.offsets),
                source_span<std::size_t>(source, metadata.bigrams->scores[0].second.offsets)};
            bigram_documents = source_span<std::byte>(source, metadata.bigrams->documents.postings);
            bigram_scores = {
                source_span<std::byte>(source, metadata.bigrams->scores[0].first.postings),
                source_span<std::byte>(source, metadata.bigrams->scores[0].second.postings)};
            auto mapping_span = source_span<std::byte>(source, metadata.bigrams->mapping);
            bigram_mapping = gsl::span<std::array<TermId, 2> const>(
                reinterpret_cast<std::array<TermId, 2> const*>(mapping_span.data()),
                mapping_span.size() / (sizeof(TermId) * 2));
            return BigramData{
                .documents = {.postings = bigram_documents, .offsets = bigram_document_offsets},
                .payloads =
                    std::array<PostingData, 2>{
                        PostingData{.postings = std::get<0>(bigram_scores),
                                    .offsets = std::get<0>(bigram_score_offsets)},
                        PostingData{.postings = std::get<1>(bigram_scores),
                                    .offsets = std::get<0>(bigram_score_offsets)}},
                .mapping = bigram_mapping};
        }
        return tl::nullopt;
    }();
    gsl::span<std::uint8_t const> quantized_max_scores;
    if (not metadata.quantized_max_scores.empty()) {
        // TODO(michal): support many precomputed scores
        for (auto [name, file] : metadata.quantized_max_scores) {
            quantized_max_scores = source_span<std::uint8_t>(source, file);
        }
    }
    return IndexRunner<DocumentReaders, PayloadReaders>(
        {.postings = documents, .offsets = document_offsets},
        {.postings = scores, .offsets = score_offsets},
        bigrams,
        document_lengths,
        tl::make_optional(metadata.avg_document_length),
        {},
        {},
        quantized_max_scores,
        std::move(source),
        std::move(document_readers),
        std::move(payload_readers));
}

} // namespace pisa::v1

namespace YAML {
template <>
struct convert<::pisa::v1::PostingFilePaths> {
    static Node encode(const ::pisa::v1::PostingFilePaths& rhs)
    {
        Node node;
        node["file"] = rhs.postings;
        node["offsets"] = rhs.offsets;
        return node;
    }

    static bool decode(const Node& node, ::pisa::v1::PostingFilePaths& rhs)
    {
        if (!node.IsMap()) {
            return false;
        }

        rhs.postings = node["file"].as<std::string>();
        rhs.offsets = node["offsets"].as<std::string>();
        return true;
    }
};

template <>
struct convert<::pisa::v1::UnigramFilePaths> {
    static Node encode(const ::pisa::v1::UnigramFilePaths& rhs)
    {
        Node node;
        node["documents"] = convert<::pisa::v1::PostingFilePaths>::encode(rhs.documents);
        node["payloads"] = convert<::pisa::v1::PostingFilePaths>::encode(rhs.payloads);
        return node;
    }

    static bool decode(const Node& node, ::pisa::v1::UnigramFilePaths& rhs)
    {
        if (!node.IsMap()) {
            return false;
        }

        rhs.documents = node["documents"].as<::pisa::v1::PostingFilePaths>();
        rhs.payloads = node["payloads"].as<::pisa::v1::PostingFilePaths>();
        return true;
    }
};

} // namespace YAML
