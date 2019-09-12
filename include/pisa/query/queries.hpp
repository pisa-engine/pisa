#pragma once

#include <iostream>
#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include <string_view>

#include <boost/algorithm/string.hpp>
#include <range/v3/view/enumerate.hpp>
#include <spdlog/spdlog.h>

#include "index_types.hpp"
#include "query/queries.hpp"
#include "scorer/score_function.hpp"
#include "term_processor.hpp"
#include "tokenizer.hpp"
#include "topk_queue.hpp"
#include "util/util.hpp"
#include "wand_data.hpp"
#include "wand_data_compressed.hpp"
#include "wand_data_raw.hpp"

namespace pisa {

using term_id_type = uint32_t;
using term_id_vec = std::vector<term_id_type>;

struct Query {
    std::optional<std::string> id;
    std::vector<term_id_type> terms;
    std::vector<float> term_weights;
};

[[nodiscard]] auto split_query_at_colon(std::string const &query_string)
    -> std::pair<std::optional<std::string>, std::string_view>
{
    // query id : terms (or ids)
    auto colon = std::find(query_string.begin(), query_string.end(), ':');
    std::optional<std::string> id;
    if (colon != query_string.end()) {
        id = std::string(query_string.begin(), colon);
    }
    auto pos = colon == query_string.end() ? query_string.begin() : std::next(colon);
    auto raw_query = std::string_view(&*pos, std::distance(pos, query_string.end()));
    return {std::move(id), std::move(raw_query)};
}

[[nodiscard]] auto parse_query_terms(std::string const &query_string, TermProcessor term_processor)
    -> Query
{
    auto [id, raw_query] = split_query_at_colon(query_string);
    TermTokenizer tokenizer(raw_query);
    std::vector<term_id_type> parsed_query;
    for (auto term_iter = tokenizer.begin(); term_iter != tokenizer.end(); ++term_iter) {
        auto raw_term = *term_iter;
        try {
            auto term = term_processor.process(std::string(raw_term));
            if (term) {
                if (!term_processor.is_stopword(*term)) {
                    parsed_query.push_back(std::move(*term));
                } else {
                    spdlog::warn("Term `{}` is a stopword and will be ignored", *term);
                }
            } else {
                spdlog::warn("Term `{}` not found and will be ignored", raw_term);
            }
        } catch (std::invalid_argument &err) {
            spdlog::warn("Could not parse `{}` to a number", raw_term);
        }
    }
    return {id, parsed_query, {}};
}

[[nodiscard]] auto parse_query_ids(std::string const &query_string) -> Query
{
    auto [id, raw_query] = split_query_at_colon(query_string);
    std::vector<term_id_type> parsed_query;
    std::vector<std::string> splitted_query;
    boost::split(splitted_query, raw_query, boost::is_any_of("\t"));
    try {
        std::transform(splitted_query.begin(),
                       splitted_query.end(),
                       std::back_inserter(parsed_query),
                       [](const std::string &val) { return std::stoi(val); });
    } catch (std::invalid_argument &err) {
        spdlog::error("Could not parse term identifiers of query `{}`", raw_query);
        exit(1);
    }
    return {id, parsed_query, {}};
}

[[nodiscard]] std::function<void(const std::string)> compute_parse_query_function(
    std::vector<Query> &queries,
    const std::optional<std::string> terms_file,
    const std::optional<std::string> stopwords_filename,
    const std::optional<std::string> stemmer_type) {
    if (terms_file) {
        auto term_processor = TermProcessor(terms_file, stopwords_filename, stemmer_type);
        return [&](std::string const &query_line) {
            queries.push_back(parse_query_terms(query_line, term_processor));
        };
    } else {
        return
            [&](std::string const &query_line) { queries.push_back(parse_query_ids(query_line)); };
    }
}

bool read_query(term_id_vec &ret, std::istream &is = std::cin)
{
    ret.clear();
    std::string line;
    if (!std::getline(is, line)) {
        return false;
    }
    ret = parse_query_ids(line).terms;
    return true;
}

void remove_duplicate_terms(term_id_vec &terms)
{
    std::sort(terms.begin(), terms.end());
    terms.erase(std::unique(terms.begin(), terms.end()), terms.end());
}

typedef std::pair<uint64_t, uint64_t> term_freq_pair;
typedef std::vector<term_freq_pair> term_freq_vec;

term_freq_vec query_freqs(term_id_vec terms)
{
    term_freq_vec query_term_freqs;
    std::sort(terms.begin(), terms.end());
    // count query term frequencies
    for (size_t i = 0; i < terms.size(); ++i) {
        if (i == 0 || terms[i] != terms[i - 1]) {
            query_term_freqs.emplace_back(terms[i], 1);
        } else {
            query_term_freqs.back().second += 1;
        }
    }
    return query_term_freqs;
}

} // namespace pisa

#include "algorithm/and_query.hpp"
#include "algorithm/block_max_maxscore_query.hpp"
#include "algorithm/block_max_ranked_and_query.hpp"
#include "algorithm/block_max_wand_query.hpp"
#include "algorithm/maxscore_query.hpp"
#include "algorithm/or_query.hpp"
#include "algorithm/range_query.hpp"
#include "algorithm/ranked_and_query.hpp"
#include "algorithm/ranked_or_query.hpp"
#include "algorithm/ranked_or_taat_query.hpp"
#include "algorithm/wand_query.hpp"
