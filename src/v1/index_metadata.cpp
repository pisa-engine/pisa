#include <fstream>
#include <optional>
#include <string>

#include <yaml-cpp/yaml.h>

#include "v1/index_metadata.hpp"

namespace pisa::v1 {

constexpr char const* DOCUMENTS = "documents";
constexpr char const* FREQUENCIES = "frequencies";
constexpr char const* SCORES = "scores";
constexpr char const* POSTINGS = "file";
constexpr char const* OFFSETS = "offsets";
constexpr char const* STATS = "stats";
constexpr char const* LEXICON = "lexicon";
constexpr char const* TERMS = "terms";
constexpr char const* BIGRAM = "bigram";

[[nodiscard]] auto resolve_yml(std::optional<std::string> const& arg) -> std::string
{
    if (arg) {
        return *arg;
    }
    throw std::runtime_error("Resolving .yml from the current folder not supported yet!");
}

[[nodiscard]] auto IndexMetadata::from_file(std::string const& file) -> IndexMetadata
{
    YAML::Node config = YAML::LoadFile(file);
    std::vector<PostingFilePaths> scores;
    if (config[SCORES]) {
        // TODO(michal): Once switched to YAML, parse an array.
        scores.push_back(PostingFilePaths{.postings = config[SCORES][POSTINGS].as<std::string>(),
                                          .offsets = config[SCORES][OFFSETS].as<std::string>()});
    }
    return IndexMetadata{
        .documents = PostingFilePaths{.postings = config[DOCUMENTS][POSTINGS].as<std::string>(),
                                      .offsets = config[DOCUMENTS][OFFSETS].as<std::string>()},
        .frequencies = PostingFilePaths{.postings = config[FREQUENCIES][POSTINGS].as<std::string>(),
                                        .offsets = config[FREQUENCIES][OFFSETS].as<std::string>()},
        .scores = std::move(scores),
        .document_lengths_path = config[STATS]["document_lengths"].as<std::string>(),
        .avg_document_length = config[STATS]["avg_document_length"].as<float>(),
        .term_lexicon = [&]() -> tl::optional<std::string> {
            if (config[LEXICON][TERMS]) {
                return config[LEXICON][TERMS].as<std::string>();
            }
            return tl::nullopt;
        }(),
        .document_lexicon = [&]() -> tl::optional<std::string> {
            if (config[LEXICON][DOCUMENTS]) {
                return config[LEXICON][DOCUMENTS].as<std::string>();
            }
            return tl::nullopt;
        }(),
        .stemmer = [&]() -> tl::optional<std::string> {
            if (config[LEXICON]["stemmer"]) {
                return config[LEXICON]["stemmer"].as<std::string>();
            }
            return tl::nullopt;
        }(),
        .bigrams = [&]() -> tl::optional<BigramMetadata> {
            if (config[BIGRAM]) {
                return BigramMetadata{
                    .documents = {.postings = config[DOCUMENTS][POSTINGS].as<std::string>(),
                                  .offsets = config[DOCUMENTS][OFFSETS].as<std::string>()},
                    .frequencies =
                        {{.postings = config["frequencies_0"][POSTINGS].as<std::string>(),
                          .offsets = config["frequencies_0"][OFFSETS].as<std::string>()},
                         {.postings = config["frequencies_1"][POSTINGS].as<std::string>(),
                          .offsets = config["frequencies_1"][OFFSETS].as<std::string>()}},
                    .mapping = config[BIGRAM]["mapping"].as<std::string>(),
                    .count = config[BIGRAM]["count"].as<std::size_t>()};
            }
            return tl::nullopt;
        }()};
}

void IndexMetadata::write(std::string const& file)
{
    YAML::Node root;
    root[DOCUMENTS][POSTINGS] = documents.postings;
    root[DOCUMENTS][OFFSETS] = documents.offsets;
    root[FREQUENCIES][POSTINGS] = frequencies.postings;
    root[FREQUENCIES][OFFSETS] = frequencies.offsets;
    root[STATS]["avg_document_length"] = avg_document_length;
    root[STATS]["document_lengths"] = document_lengths_path;
    root[LEXICON]["stemmer"] = "porter2";
    if (not scores.empty()) {
        root[SCORES][POSTINGS] = scores.front().postings;
        root[SCORES][OFFSETS] = scores.front().offsets;
    }
    if (term_lexicon) {
        root[LEXICON][TERMS] = *term_lexicon;
    }
    if (document_lexicon) {
        root[LEXICON][DOCUMENTS] = *document_lexicon;
    }
    if (bigrams) {
        root[BIGRAM][DOCUMENTS][POSTINGS] = bigrams->documents.postings;
        root[BIGRAM][DOCUMENTS][OFFSETS] = bigrams->documents.offsets;
        root[BIGRAM]["frequencies_0"][POSTINGS] = bigrams->frequencies.first.postings;
        root[BIGRAM]["frequencies_0"][OFFSETS] = bigrams->frequencies.first.offsets;
        root[BIGRAM]["frequencies_1"][POSTINGS] = bigrams->frequencies.second.postings;
        root[BIGRAM]["frequencies_1"][OFFSETS] = bigrams->frequencies.second.offsets;
        root[BIGRAM]["mapping"] = bigrams->mapping;
        root[BIGRAM]["count"] = bigrams->count;
    }
    std::ofstream fout(file);
    fout << root;
}

} // namespace pisa::v1
