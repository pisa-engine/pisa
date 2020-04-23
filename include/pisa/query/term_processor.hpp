#pragma once

#include <functional>
#include <memory>
#include <optional>
#include <spdlog/spdlog.h>
#include <unordered_set>

#include <KrovetzStemmer/KrovetzStemmer.hpp>
#include <Porter2.hpp>
#include <boost/algorithm/string.hpp>
#include <mio/mmap.hpp>

#include "io.hpp"
#include "payload_vector.hpp"

namespace pisa {

using term_id_type = uint32_t;
using Stemmer_t = std::function<std::string(std::string)>;
auto term_processor = [](std::optional<std::string> const& type) -> Stemmer_t {
    if (not type) {
        return [](std::string&& term) -> std::string {
            boost::algorithm::to_lower(term);
            return std::move(term);
        };
    }
    if (*type == "porter2") {
        return [](std::string&& term) -> std::string {
            boost::algorithm::to_lower(term);
            return porter2::Stemmer{}.stem(term);
        };
    }
    if (*type == "krovetz") {
        static stem::KrovetzStemmer kstemmer;
        return [](std::string&& term) -> std::string {
            boost::algorithm::to_lower(term);
            return kstemmer.kstem_stemmer(term);
        };
    }
    throw std::invalid_argument(fmt::format("Unknown stemmer type: {}", *type));
};

class TermProcessor {
  private:
    std::unordered_set<term_id_type> stopwords;

    // Method implemented in constructor according to the specified stemmer.
    std::function<std::optional<term_id_type>(std::string)> _to_id;

  public:
    TermProcessor(
        std::optional<std::string> const& terms_file,
        std::optional<std::string> const& stopwords_filename,
        std::optional<std::string> const& stemmer_type)
    {
        auto source = std::make_shared<mio::mmap_source>(terms_file->c_str());
        auto terms = Payload_Vector<>::from(*source);
        auto to_id = [source = std::move(source), terms](auto str) -> std::optional<term_id_type> {
            // Note: the lexicographical order of the terms matters.
            auto pos = std::lower_bound(terms.begin(), terms.end(), std::string_view(str));
            if (*pos == std::string_view(str)) {
                return std::distance(terms.begin(), pos);
            }
            return std::nullopt;
        };

        // Implements '_to_id' method.
        _to_id = [=](auto str) { return to_id(term_processor(stemmer_type)(str)); };
        // Loads stopwords.
        if (stopwords_filename) {
            std::ifstream is(*stopwords_filename);
            io::for_each_line(is, [&](auto&& word) {
                if (auto processed_term = _to_id(std::move(word)); processed_term.has_value()) {
                    stopwords.insert(*processed_term);
                }
            });
        }
    }

    std::optional<term_id_type> operator()(std::string token) { return _to_id(token); }

    bool is_stopword(const term_id_type term) { return stopwords.find(term) != stopwords.end(); }

    std::vector<term_id_type> get_stopwords()
    {
        std::vector<term_id_type> v;
        v.insert(v.end(), stopwords.begin(), stopwords.end());
        sort(v.begin(), v.end());
        return v;
    }
};
}  // namespace pisa
