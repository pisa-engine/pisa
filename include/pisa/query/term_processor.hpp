#pragma once

#include <functional>
#include <optional>
#include <unordered_set>

#include <KrovetzStemmer/KrovetzStemmer.hpp>
#include <Porter2/Porter2.hpp>
#include <boost/algorithm/string.hpp>
#include <mio/mmap.hpp>

#include "io.hpp"
#include "payload_vector.hpp"

namespace pisa {

using term_id_type = uint32_t;

class TermProcessor {
   private:
    std::unordered_set<term_id_type> stopwords;

   public:
    // Method implemented in constructor according to the specified stemmer.
    std::function<std::optional<term_id_type>(std::string)> process;

    TermProcessor(std::optional<std::string> terms_file,
                  std::optional<std::string> stopwords_filename,
                  std::optional<std::string> stemmer_type)
    {
        auto source = std::make_shared<mio::mmap_source>(terms_file->c_str());
        auto terms = Payload_Vector<>::from(*source);
        auto to_id = [source = std::move(source),
                      terms = std::move(terms)](auto str) -> std::optional<term_id_type> {
            // Note: the lexicographical order of the terms matters.
            auto pos = std::lower_bound(terms.begin(), terms.end(), std::string_view(str));
            if (*pos == std::string_view(str)) {
                return std::distance(terms.begin(), pos);
            }
            return std::nullopt;
        };

        // Implements 'process' method.
        if (not stemmer_type) {
            process = [=](auto str) {
                boost::algorithm::to_lower(str);
                return to_id(str);
            };
        } else if (*stemmer_type == "porter2") {
            process = [=](auto str) {
                boost::algorithm::to_lower(str);
                stem::Porter2 stemmer{};
                return to_id(stemmer.stem(str));
            };
        } else if (*stemmer_type == "krovetz") {
            process = [=](auto str) {
                boost::algorithm::to_lower(str);
                stem::KrovetzStemmer stemmer{};
                stemmer.kstem_stemmer(str);
                return to_id(stemmer.kstem_stemmer(str));
            };
        } else {
            throw std::invalid_argument("Unknown stemmer");
        }

        // Loads stopwords.
        if (stopwords_filename) {
            std::ifstream is(*stopwords_filename);
            io::for_each_line(is, [&](auto &&word) {
                if (auto processed_term = process(std::move(word)); processed_term.has_value()) {
                    stopwords.insert(*processed_term);
                }
            });
        }
    }

    bool is_stopword(term_id_type term) { return stopwords.find(term) != stopwords.end(); }

    std::vector<term_id_type> get_stopwords()
    {
        std::vector<term_id_type> v;
        v.insert(v.end(), stopwords.begin(), stopwords.end());
        sort(v.begin(), v.end());
        return v;
    }
};
} // namespace pisa