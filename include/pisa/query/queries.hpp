#pragma once

#include <functional>
#include <iostream>
#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include <string_view>
#include <unordered_map>

#include "index_types.hpp"
#include "io.hpp"
#include "payload_vector.hpp"
#include "query/queries.hpp"
#include "tokenizer/term_tokenizer.hpp"
#include "topk_queue.hpp"
#include "util/util.hpp"
#include "wand_data.hpp"
#include "wand_data_compressed.hpp"
#include "wand_data_raw.hpp"
#include <KrovetzStemmer/KrovetzStemmer.hpp>
#include <Porter2.hpp>
#include <boost/algorithm/string.hpp>
#include <mio/mmap.hpp>
#include <range/v3/view/enumerate.hpp>
#include <spdlog/spdlog.h>

namespace pisa {

using term_id_type = uint32_t;
using term_id_vec = std::vector<term_id_type>;
using TermProcessor = std::function<std::optional<term_id_type>(std::string &&)>;

struct Query {
    std::optional<std::string> id;
    std::vector<term_id_type> terms;
    std::vector<float> term_weights;
};

using ResultVector = std::vector<std::pair<float, std::uint64_t>>;
using QueryExecutor = std::function<ResultVector(Query)>;

[[nodiscard]] inline auto parse_query(
    std::string const &query_string,
    TermProcessor process_term,
    std::optional<std::unordered_set<term_id_type>> const &stopwords = std::nullopt) -> Query
{
    std::optional<std::string> id = std::nullopt;
    std::vector<term_id_type> parsed_query;
    auto colon = std::find(query_string.begin(), query_string.end(), ':');
    if (colon != query_string.end()) {
        id = std::string(query_string.begin(), colon);
    }
    auto pos = colon == query_string.end() ? query_string.begin() : std::next(colon);
    auto raw_query = std::string_view(&*pos, std::distance(pos, query_string.end()));
    tok::TermTokenizer tokenizer(raw_query);
    for (auto term_iter = tokenizer.begin(); term_iter != tokenizer.end(); ++term_iter) {
        auto raw_term = *term_iter;
        try {
            auto processed = process_term(std::string(raw_term));
            if (processed) {
                if (not stopwords or stopwords->find(*processed) == stopwords->end()) {
                    parsed_query.push_back(std::move(*processed));
                } else {
                    spdlog::warn("Term `{}` is a stop word and will be ignored", *processed);
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

inline bool read_query(
    term_id_vec &ret,
    std::istream &is = std::cin,
    std::function<term_id_type(std::string)> process_term = [](auto str) { return std::stoi(str); })
{
    ret.clear();
    std::string line;
    if (!std::getline(is, line)) {
        return false;
    }
    ret = parse_query(line, process_term).terms;
    return true;
}

inline void remove_duplicate_terms(term_id_vec &terms)
{
    std::sort(terms.begin(), terms.end());
    terms.erase(std::unique(terms.begin(), terms.end()), terms.end());
}

typedef std::pair<uint64_t, uint64_t> term_freq_pair;
typedef std::vector<term_freq_pair> term_freq_vec;

inline term_freq_vec query_freqs(term_id_vec terms)
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

namespace query {

    inline TermProcessor term_processor(std::optional<std::string> terms_file,
                                        std::optional<std::string> stemmer_type)
    {
        if (terms_file) {
            auto source = std::make_shared<mio::mmap_source>(terms_file->c_str());
            auto terms = Payload_Vector<>::from(*source);
            auto to_id = [source = std::move(source),
                          terms = std::move(terms)](auto str) -> std::optional<term_id_type> {
                auto pos = std::lower_bound(terms.begin(), terms.end(), std::string_view(str));
                if (*pos == std::string_view(str)) {
                    return std::distance(terms.begin(), pos);
                }
                return std::nullopt;
            };
            if (not stemmer_type) {
                return [=](auto str) {
                    boost::algorithm::to_lower(str);
                    return to_id(str);
                };
            }
            if (*stemmer_type == "porter2") {
                return [=](auto str) {
                    boost::algorithm::to_lower(str);
                    porter2::Stemmer stemmer{};
                    return to_id(stemmer.stem(str));
                };
            }
            if (*stemmer_type == "krovetz") {
                return [=](auto str) {
                    boost::algorithm::to_lower(str);
                    stem::KrovetzStemmer stemmer{};
                    return to_id(stemmer.kstem_stemmer(str));
                };
            }
            throw std::invalid_argument("Unknown stemmer");
        } else {
            return [](auto str) { return std::make_optional<term_id_type>(std::stoi(str)); };
        }
    }

} // namespace query
} // namespace pisa
