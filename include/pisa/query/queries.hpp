#pragma once

#include <functional>
#include <optional>
#include <string>
#include <unordered_set>
#include <vector>

namespace pisa {

using term_id_type = uint32_t;
using term_id_vec = std::vector<term_id_type>;
using TermProcessor = std::function<std::optional<term_id_type>(std::string &&)>;
using term_freq_pair = std::pair<uint64_t, uint64_t>;
using term_freq_vec = std::vector<term_freq_pair>;

struct Query {
    std::optional<std::string> id;
    std::vector<term_id_type> terms;
    std::vector<float> term_weights;
};

[[nodiscard]] auto parse_query(
    std::string const &query_string,
    TermProcessor process_term,
    std::optional<std::unordered_set<term_id_type>> const &stopwords = std::nullopt) -> Query;

[[nodiscard]] auto query_freqs(term_id_vec terms) -> term_freq_vec;
void remove_duplicate_terms(term_id_vec &terms);

namespace query {

    [[nodiscard]] auto term_processor(std::optional<std::string> terms_file,
                                      std::optional<std::string> stemmer_type) -> TermProcessor;

} // namespace query
} // namespace pisa
