#pragma once

#include <functional>
#include <optional>
#include <unordered_set>

#include "io.hpp"
#include "memory_source.hpp"
#include "payload_vector.hpp"

namespace pisa {

using term_id_type = uint32_t;
using Stemmer_t = std::function<std::string(std::string)>;

auto term_processor_builder(std::optional<std::string> const& type) -> std::function<Stemmer_t()>;

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
        auto source = std::make_shared<MemorySource>(MemorySource::mapped_file(*terms_file));
        auto terms = Payload_Vector<>::from(*source);
        auto to_id = [source = std::move(source), terms](auto str) -> std::optional<term_id_type> {
            // Note: the lexicographical order of the terms matters.
            return pisa::binary_search(terms.begin(), terms.end(), std::string_view(str));
        };

        // Implements '_to_id' method.
        _to_id = [=](auto str) { return to_id(term_processor_builder(stemmer_type)()(str)); };
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
