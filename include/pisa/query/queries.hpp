#pragma once

#include <iostream>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "query/term_processor.hpp"
#include "tokenizer.hpp"

namespace pisa {

using term_id_type = uint32_t;
using term_id_vec = std::vector<term_id_type>;
using term_freq_pair = std::pair<uint64_t, uint64_t>;
using term_freq_vec = std::vector<term_freq_pair>;

struct Query {
    std::optional<std::string> id;
    std::vector<term_id_type> terms;
    std::vector<float> term_weights;
};

[[nodiscard]] auto split_query_at_colon(std::string const& query_string)
    -> std::pair<std::optional<std::string>, std::string_view>;

[[nodiscard]] auto parse_query_terms(
    std::string const& query_string, Tokenizer const& tokenizer, TermProcessor term_processor)
    -> Query;

[[nodiscard]] auto parse_query_ids(std::string const& query_string) -> Query;

[[nodiscard]] std::function<void(const std::string)> resolve_query_parser(
    std::vector<Query>& queries,
    std::unique_ptr<pisa::Tokenizer> tokenizer,
    std::optional<std::string> const& terms_file,
    std::optional<std::string> const& stopwords_filename,
    std::optional<std::string> const& stemmer_type);

bool read_query(term_id_vec& ret, std::istream& is = std::cin);

void remove_duplicate_terms(term_id_vec& terms);

term_freq_vec query_freqs(term_id_vec terms);

}  // namespace pisa
