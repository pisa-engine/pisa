#include "query/term_processor.hpp"

#include <KrovetzStemmer/KrovetzStemmer.hpp>
#include <Porter2.hpp>
#include <boost/algorithm/string.hpp>

namespace pisa {

using term_id_type = uint32_t;
using Stemmer_t = std::function<std::string(std::string)>;

auto term_processor_builder(std::optional<std::string> const& type) -> std::function<Stemmer_t()>
{
    if (not type) {
        return [] {
            return [](std::string&& term) -> std::string {
                boost::algorithm::to_lower(term);
                return std::move(term);
            };
        };
    }
    if (*type == "porter2") {
        return [] {
            return [](std::string&& term) -> std::string {
                boost::algorithm::to_lower(term);
                return porter2::Stemmer{}.stem(term);
            };
        };
    }
    if (*type == "krovetz") {
        return []() {
            return [kstemmer = std::make_shared<stem::KrovetzStemmer>()](
                       std::string&& term) mutable -> std::string {
                boost::algorithm::to_lower(term);
                return kstemmer->kstem_stemmer(term);
            };
        };
    }
    throw std::invalid_argument(fmt::format("Unknown stemmer type: {}", *type));
};

}  // namespace pisa
