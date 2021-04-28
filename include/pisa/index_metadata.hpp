#pragma once

#include <filesystem>
#include <functional>
#include <map>
#include <string>

#include <mio/mmap.hpp>

namespace pisa {

struct Query;

namespace config {

    /// Available stermmer types.
    enum class Stemmer { Porter2 };

    /// Available scorers.
    enum class Scorer { BM25, QueryLikelihood };

} // namespace config

/// Any posting data, such as documents, frequencies, scores, or block max scores,
/// have a postings file that contains data for one posting list after another,
/// and an offset file.
struct PostingFilePaths {
    std::filesystem::path postings;
    std::filesystem::path offsets;
};

/// Index metadata stores required paths and properties of an index.
struct IndexMetadata {
    /// Path to the metadata file. I suggest YAML.
    std::optional<std::filesystem::path> meta_file;

    /// Path to a file containing document lengths.
    std::filesystem::path document_lengths;

    /// Stemmer with which the collection was parsed.
    std::optional<config::Stemmer> stemmer;

    /// Lexicon paths
    std::optional<std::filesystem::path> term_lexicon;
    std::optional<std::filesystem::path> document_lexicon;

    /// Statistics
    float avg_document_length;
    std::size_t document_count;
    std::size_t posting_count;
    std::size_t term_count;

    /// Write this metadata to a file.
    void write(std::filesystem::path const& file) const;

    /// Update the same file it was loaded from. Will throw if `meta_file` is not set.
    void update() const;

    /// Loads metadata from a YAML file.
    [[nodiscard]] static auto from_file(std::filesystem::path const& file) -> IndexMetadata;

    /// Returns query parser with appropriate stemming.
    [[nodiscard]] auto query_parser(std::optional<std::filesystem::path> const& stop_words =
                                        std::nullopt) const -> std::function<Query(std::string)>;
};

struct DaatIndexMetadata : public IndexMetadata {
    /// Paths to documents.
    PostingFilePaths documents;
    /// Paths to frequencies.
    PostingFilePaths frequencies;
    /// Optional paths to scores.
    std::map<config::Scorer, PostingFilePaths> quantized_scores;

    /// Term max scores
    std::map<config::Scorer, std::filesystem::path> max_scores;
    std::map<config::Scorer, std::filesystem::path> quantized_max_scores;

    /// Block max scores
    std::map<config::Scorer, PostingFilePaths> block_max_scores;
    std::map<config::Scorer, PostingFilePaths> quantized_block_max_scores;
};

struct SaatIndexMetadata : public IndexMetadata {
    /// SAAT index has postings in one place. We could technically separate scores and documents
    /// but seeing that scores are an intrinsic part of a posting list block, maybe it's better
    /// to leave them there. It's not like we can use different ones with the same postings,
    /// because it would change the order.
    PostingFilePaths postings;
};

/// This is a helpful class that makes it easy to create memory mapped files for index.
class MMapSource {
   public:
    MMapSource() = default;
    MMapSource(MMapSource&&) = default;
    MMapSource(MMapSource const&) = default;
    MMapSource& operator=(MMapSource&&) = default;
    MMapSource& operator=(MMapSource const&) = default;
    ~MMapSource() = default;

   private:
    std::vector<std::shared_ptr<mio::mmap_source>> file_sources{};
};

} // namespace pisa
