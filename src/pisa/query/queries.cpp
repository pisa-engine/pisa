#include <string_view>

#include <boost/algorithm/string.hpp>
#include <mio/mmap.hpp>
#include <spdlog/spdlog.h>

#include "parsing/stem.hpp"
#include "payload_vector.hpp"
#include "query/queries.hpp"
#include "tokenizer.hpp"

namespace pisa {

[[nodiscard]] auto parse_query(std::string const &query_string,
                               TermProcessor process_term,
                               std::optional<std::unordered_set<term_id_type>> const &stopwords)
    -> Query
{
    std::optional<std::string> id = std::nullopt;
    std::vector<term_id_type> parsed_query;
    auto colon = std::find(query_string.begin(), query_string.end(), ':');
    if (colon != query_string.end()) {
        id = std::string(query_string.begin(), colon);
    }
    auto pos = colon == query_string.end() ? query_string.begin() : std::next(colon);
    auto raw_query = std::string_view(&*pos, std::distance(pos, query_string.end()));
    TermTokenizer tokenizer(raw_query);
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

void remove_duplicate_terms(term_id_vec &terms)
{
    std::sort(terms.begin(), terms.end());
    terms.erase(std::unique(terms.begin(), terms.end()), terms.end());
}

[[nodiscard]] auto query_freqs(term_id_vec terms) -> term_freq_vec
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

[[nodiscard]] auto query::term_processor(std::optional<std::string> terms_file,
                                         std::optional<std::string> stemmer_type) -> TermProcessor
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
                return to_id(porter2::stem(str));
            };
        }
        if (*stemmer_type == "krovetz") {
            return [=](auto str) {
                boost::algorithm::to_lower(str);
                return to_id(krovetz::stem(str));
            };
        }
        throw std::invalid_argument("Unknown stemmer");
    } else {
        return [](auto str) { return std::make_optional<term_id_type>(std::stoi(str)); };
    }
}

} // namespace pisa
