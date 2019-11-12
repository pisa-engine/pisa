#include <optional>
#include <string>

#include <boost/property_tree/ini_parser.hpp>
#include <boost/property_tree/ptree.hpp>

#include "v1/index_metadata.hpp"

namespace pisa::v1 {

[[nodiscard]] auto resolve_ini(std::optional<std::string> const &arg) -> std::string
{
    if (arg) {
        return *arg;
    }
    throw std::runtime_error("Resolving .ini from the current folder not supported yet!");
}

[[nodiscard]] auto IndexMetadata::from_file(std::string const &file) -> IndexMetadata
{
    boost::property_tree::ptree pt;
    boost::property_tree::ini_parser::read_ini(file, pt);
    std::vector<PostingFilePaths> scores;
    if (pt.count("scores") > 0U) {
        scores.push_back(PostingFilePaths{.postings = pt.get<std::string>("scores.file"),
                                          .offsets = pt.get<std::string>("scores.offsets")});
    }
    return IndexMetadata{
        .documents = PostingFilePaths{.postings = pt.get<std::string>("documents.file"),
                                      .offsets = pt.get<std::string>("documents.offsets")},
        .frequencies = PostingFilePaths{.postings = pt.get<std::string>("frequencies.file"),
                                        .offsets = pt.get<std::string>("frequencies.offsets")},
        // TODO(michal): Once switched to YAML, parse an array.
        .scores = std::move(scores),
        .document_lengths_path = pt.get<std::string>("stats.document_lengths"),
        .avg_document_length = pt.get<float>("stats.avg_document_length"),
        .term_lexicon = convert_optional(pt.get_optional<std::string>("lexicon.terms")),
        .document_lexicon = convert_optional(pt.get_optional<std::string>("lexicon.documents")),
        .stemmer = convert_optional(pt.get_optional<std::string>("lexicon.stemmer"))};
}

void IndexMetadata::write(std::string const &file)
{
    boost::property_tree::ptree pt;
    pt.put("documents.file", documents.postings);
    pt.put("documents.offsets", documents.offsets);
    pt.put("frequencies.file", frequencies.postings);
    pt.put("frequencies.offsets", frequencies.offsets);
    pt.put("stats.avg_document_length", avg_document_length);
    pt.put("stats.document_lengths", document_lengths_path);
    pt.put("lexicon.stemmer", "porter2"); // TODO(michal): Parametrize
    if (not scores.empty()) {
        pt.put("scores.file", scores.front().postings);
        pt.put("scores.offsets", scores.front().offsets);
    }
    if (term_lexicon) {
        pt.put("lexicon.terms", *term_lexicon);
    }
    if (document_lexicon) {
        pt.put("lexicon.documents", *document_lexicon);
    }
    boost::property_tree::write_ini(file, pt);
}

} // namespace pisa::v1
