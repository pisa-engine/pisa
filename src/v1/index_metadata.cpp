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
    return IndexMetadata{
        .documents = PostingFilePaths{.postings = pt.get<std::string>("documents.file"),
                                      .offsets = pt.get<std::string>("documents.offsets")},
        .frequencies = PostingFilePaths{.postings = pt.get<std::string>("frequencies.file"),
                                        .offsets = pt.get<std::string>("frequencies.offsets")},
        .document_lengths_path = pt.get<std::string>("stats.document_lengths"),
        .avg_document_length = pt.get<float>("stats.avg_document_length"),
        .term_lexicon = convert_optional(pt.get_optional<std::string>("lexicon.terms")),
        .document_lexicon = convert_optional(pt.get_optional<std::string>("lexicon.documents")),
        .stemmer = convert_optional(pt.get_optional<std::string>("lexicon.stemmer"))};
}

} // namespace pisa::v1
