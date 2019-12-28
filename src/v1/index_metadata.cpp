#include <fstream>
#include <optional>
#include <string>

#include <yaml-cpp/yaml.h>

#include "query/queries.hpp"
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
constexpr char const* MAX_SCORES = "max_scores";
constexpr char const* BLOCK_MAX_SCORES = "block_max_scores";
constexpr char const* QUANTIZED_MAX_SCORES = "quantized_max_scores";

[[nodiscard]] auto resolve_yml(tl::optional<std::string> const& arg) -> std::string
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
        .basename = file.substr(0, file.size() - 4),
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
                std::vector<std::pair<PostingFilePaths, PostingFilePaths>> scores;
                if (config[BIGRAM]["scores_0"]) {
                    scores = {{{.postings = config[BIGRAM]["scores_0"][POSTINGS].as<std::string>(),
                                .offsets = config[BIGRAM]["scores_0"][OFFSETS].as<std::string>()},
                               {.postings = config[BIGRAM]["scores_1"][POSTINGS].as<std::string>(),
                                .offsets = config[BIGRAM]["scores_1"][OFFSETS].as<std::string>()}}};
                }
                return BigramMetadata{
                    .documents = {.postings = config[BIGRAM][DOCUMENTS][POSTINGS].as<std::string>(),
                                  .offsets = config[BIGRAM][DOCUMENTS][OFFSETS].as<std::string>()},
                    .frequencies =
                        {{.postings = config[BIGRAM]["frequencies_0"][POSTINGS].as<std::string>(),
                          .offsets = config[BIGRAM]["frequencies_0"][OFFSETS].as<std::string>()},
                         {.postings = config[BIGRAM]["frequencies_1"][POSTINGS].as<std::string>(),
                          .offsets = config[BIGRAM]["frequencies_1"][OFFSETS].as<std::string>()}},
                    .scores = std::move(scores),
                    .mapping = config[BIGRAM]["mapping"].as<std::string>(),
                    .count = config[BIGRAM]["count"].as<std::size_t>()};
            }
            return tl::nullopt;
        }(),
        .max_scores =
            [&]() {
                if (config[MAX_SCORES]) {
                    return config[MAX_SCORES].as<std::map<std::string, std::string>>();
                }
                return std::map<std::string, std::string>{};
            }(),
        .block_max_scores =
            [&]() {
                if (config[BLOCK_MAX_SCORES]) {
                    return config[BLOCK_MAX_SCORES].as<std::map<std::string, UnigramFilePaths>>();
                }
                return std::map<std::string, UnigramFilePaths>{};
            }(),
        .quantized_max_scores =
            [&]() {
                if (config[QUANTIZED_MAX_SCORES]) {
                    return config[QUANTIZED_MAX_SCORES].as<std::map<std::string, std::string>>();
                }
                return std::map<std::string, std::string>{};
            }()};
}

void IndexMetadata::update() const { write(fmt::format("{}.yml", get_basename())); }

void IndexMetadata::write(std::string const& file) const
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
        if (not bigrams->scores.empty()) {
            root[BIGRAM]["scores_0"][POSTINGS] = bigrams->scores.front().first.postings;
            root[BIGRAM]["scores_0"][OFFSETS] = bigrams->scores.front().first.offsets;
            root[BIGRAM]["scores_1"][POSTINGS] = bigrams->scores.front().second.postings;
            root[BIGRAM]["scores_1"][OFFSETS] = bigrams->scores.front().second.offsets;
        }
        root[BIGRAM]["mapping"] = bigrams->mapping;
        root[BIGRAM]["count"] = bigrams->count;
    }
    if (not max_scores.empty()) {
        for (auto [key, value] : max_scores) {
            root[MAX_SCORES][key] = value;
        }
    }
    if (not block_max_scores.empty()) {
        for (auto [key, value] : block_max_scores) {
            root[BLOCK_MAX_SCORES][key] = value;
        }
    }
    if (not quantized_max_scores.empty()) {
        for (auto [key, value] : quantized_max_scores) {
            root[QUANTIZED_MAX_SCORES][key] = value;
        }
    }
    std::ofstream fout(file);
    fout << root;
}

[[nodiscard]] auto IndexMetadata::query_parser() const -> std::function<void(Query&)>
{
    if (term_lexicon) {
        auto term_processor =
            ::pisa::TermProcessor(*term_lexicon, {}, [&]() -> std::optional<std::string> {
                if (stemmer) {
                    return *stemmer;
                }
                return std::nullopt;
            }());
        return [term_processor = std::move(term_processor)](Query& query) {
            query.term_ids(parse_query_terms(query.get_raw(), term_processor).terms);
        };
    }
    throw std::runtime_error("Unable to parse query: undefined term lexicon");
}

[[nodiscard]] auto IndexMetadata::get_basename() const -> std::string const&
{
    if (not basename) {
        throw std::runtime_error("Unable to resolve index basename");
    }
    return basename.value();
}

} // namespace pisa::v1
