#pragma once

#include <functional>
#include <iostream>
#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include <string_view>
#include <unordered_map>

#include <KrovetzStemmer/KrovetzStemmer.hpp>
#include <Porter2/Porter2.hpp>
#include <boost/algorithm/string.hpp>
#include <mio/mmap.hpp>
#include <range/v3/view/enumerate.hpp>
#include <spdlog/spdlog.h>

#include "index_types.hpp"
#include "io.hpp"
#include "payload_vector.hpp"
#include "query/queries.hpp"
#include "scorer/score_function.hpp"
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
    std::vector<term_id_type>  terms;
    std::vector<float>  term_weights;
};

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
            terms = std::move(terms)](auto str) -> std::optional<term_id_type>
            {
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
            }
            else if (*stemmer_type == "porter2") {
                process = [=](auto str) {
                    boost::algorithm::to_lower(str);
                    stem::Porter2 stemmer{};
                    return to_id(stemmer.stem(str));
                };
            }
            else if (*stemmer_type == "krovetz") {
                process = [=](auto str) {
                    boost::algorithm::to_lower(str);
                    stem::KrovetzStemmer stemmer{};
                    stemmer.kstem_stemmer(str);
                    return to_id(stemmer.kstem_stemmer(str));
                };
            }
            else
            {
                throw std::invalid_argument("Unknown stemmer");
            }

            // Loads stopwords.
            if (stopwords_filename) {
                std::ifstream is(*stopwords_filename);
                io::for_each_line(is, [&](auto &&word) {
                    if (auto processed_term = process(std::move(word)); process) {
                        stopwords.insert(*processed_term);
                    }
                });
            }
        }

        bool is_stopword(term_id_type term)
        {
            return stopwords.find(term) != stopwords.end();
        }
};

[[nodiscard]] auto query_to_raw(std::string query_string, std::optional<std::string> &id)
{
    // query id : terms (or ids)
    auto colon = std::find(query_string.begin(), query_string.end(), ':');
    if (colon != query_string.end()) {
        id = std::string(query_string.begin(), colon);
    }
    auto pos = colon == query_string.end() ? query_string.begin() : std::next(colon);
    auto raw_query = std::string(&*pos, std::distance(pos, query_string.end()));
    return raw_query;
}

[[nodiscard]] auto parse_query_terms(std::string const &query_string, TermProcessor term_processor) -> Query
{
    std::optional<std::string> id = std::nullopt;
    std::vector<term_id_type> parsed_query;
    auto raw_query = query_to_raw(query_string, id);
    TermTokenizer tokenizer(raw_query);
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
        } catch (std::invalid_argument& err) {
            spdlog::warn("Could not parse `{}` to a number", raw_term);
        }
    }
    return {id, parsed_query, {}};
}

[[nodiscard]] auto parse_query_ids(std::string const &query_string) -> Query
{
    std::optional<std::string> id = std::nullopt;
    std::vector<term_id_type> parsed_query;
    auto raw_query = query_to_raw(query_string, id);
    std::vector<std::string> splitted_query;
    boost::split(splitted_query, raw_query, boost::is_any_of("\t"));
    try {
        std::transform(splitted_query.begin(), splitted_query.end(), std::back_inserter(parsed_query),
            [](const std::string& val) {return std::stoi(val);});
    } catch (std::invalid_argument& err) {
        spdlog::error("Could not parse term identifiers of query `{}`", raw_query);
        exit(1);
    }
    return {id, parsed_query, {}};
}

[[nodiscard]] std::function<void(std::string)> compute_parse_query_function(
    std::vector<Query> &queries,
    std::optional<std::string> terms_file,
    std::optional<std::string> stopwords_filename,
    std::optional<std::string> stemmer_type) {
    if (terms_file) {
        auto term_processor = TermProcessor(terms_file, stopwords_filename, stemmer_type);
        return [&](std::string const &query_line) {
            queries.push_back(parse_query_terms(query_line, term_processor));
        };
    }
    else {
        return [&](std::string const &query_line) {
            queries.push_back(parse_query_ids(query_line));
        };
    }
}

bool read_query(term_id_vec &ret, std::istream &is = std::cin) {
    ret.clear();
    std::string line;
    if (!std::getline(is, line)) {
        return false;
    }
    ret = parse_query_ids(line).terms;
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
