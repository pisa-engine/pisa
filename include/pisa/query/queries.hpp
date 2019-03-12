#pragma once

#include <functional>
#include <iostream>
#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include <unordered_map>

#include <Porter2/Porter2.hpp>
#include <range/v3/view/enumerate.hpp>
#include <spdlog/spdlog.h>

#include "index_types.hpp"
#include "io.hpp"
#include "query/queries.hpp"
#include "topk_queue.hpp"
#include "util/util.hpp"
#include "wand_data.hpp"
#include "wand_data_compressed.hpp"
#include "wand_data_raw.hpp"
#include "scorer/score_function.hpp"

namespace pisa {

using term_id_type = uint32_t;
using term_id_vec = std::vector<term_id_type>;

struct Query {
    std::optional<std::string> id;
    std::vector<term_id_type> terms;
};

[[nodiscard]] inline auto parse_query(std::string const &query_string,
                                      std::function<term_id_type(std::string)> process_term)
    -> Query
{
    std::optional<std::string> id = std::nullopt;
    std::vector<term_id_type> parsed_query;
    auto colon = std::find(query_string.begin(), query_string.end(), ':');
    if (colon != query_string.end()) {
        id = std::string(query_string.begin(), colon);
    }
    auto pos = colon == query_string.end() ? query_string.begin() : std::next(colon);
    std::istringstream iline(std::string(pos, query_string.end()));
    std::string term;
    while (iline >> term) {
        try {
            parsed_query.push_back(process_term(term));
        } catch (std::invalid_argument& err) {
            spdlog::warn("Could not parse `{}` to a number", term);
        } catch (std::out_of_range& err) {
            spdlog::warn("Term `{}` not found and will be ignored", term);
        }
    }
    return {id, parsed_query};
}

bool read_query(term_id_vec &ret,
                std::istream &is = std::cin,
                std::function<term_id_type(std::string)> process_term = [](auto str) {
                    return std::stoi(str);
                })
{
    ret.clear();
    std::string line;
    if (!std::getline(is, line)) {
        return false;
    }
    ret = parse_query(line, process_term).terms;
    return true;
}

void remove_duplicate_terms(term_id_vec &terms) {
    std::sort(terms.begin(), terms.end());
    terms.erase(std::unique(terms.begin(), terms.end()), terms.end());
}

typedef std::pair<uint64_t, uint64_t> term_freq_pair;
typedef std::vector<term_freq_pair>   term_freq_vec;

term_freq_vec query_freqs(term_id_vec terms) {
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

// TODO: These are functions common to query processing in general.
//       They should be moved out of this file.
namespace query {

std::function<term_id_type(std::string &&)> term_processor(std::optional<std::string> terms_file,
                                                           bool stem)
{
    if (terms_file) {
        auto to_id = [m = std::make_shared<std::unordered_map<std::string, term_id_type>>(
                          io::read_string_map<term_id_type>(*terms_file))](auto str) {
            return m->at(str);
        };
        if (stem) {
            return [=](auto str) {
                stem::Porter2 stemmer{};
                return to_id(stemmer.stem(str));
            };
        } else {
            return to_id;
        }
    }
    else {
        return [](auto str) { return std::stoi(str); };
    }
}

} // namespace query
} // namespace pisa

#include "algorithm/and_query.hpp"
#include "algorithm/block_max_maxscore_query.hpp"
#include "algorithm/block_max_wand_query.hpp"
#include "algorithm/maxscore_query.hpp"
#include "algorithm/or_query.hpp"
#include "algorithm/ranked_and_query.hpp"
#include "algorithm/ranked_or_query.hpp"
#include "algorithm/wand_query.hpp"
#include "algorithm/ranked_or_taat_query.hpp"
#include "algorithm/range_query.hpp"