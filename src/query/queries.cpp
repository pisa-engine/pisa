#include "query/queries.hpp"

#include <boost/algorithm/string.hpp>
#include <range/v3/view/enumerate.hpp>
#include <spdlog/spdlog.h>

#include "index_types.hpp"
#include "tokenizer.hpp"
#include "topk_queue.hpp"
#include "util/util.hpp"

namespace pisa {

auto split_query_at_colon(std::string const& query_string)
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
    return {std::move(id), raw_query};
}

auto parse_query_terms(
    std::string const& query_string, Tokenizer const& tokenizer, TermProcessor term_processor) -> Query
{
    auto [id, raw_query] = split_query_at_colon(query_string);
    auto tokens = tokenizer.tokenize(raw_query);
    std::vector<term_id_type> parsed_query;
    for (auto raw_term: *tokens) {
        auto term = term_processor(raw_term);
        if (term) {
            if (!term_processor.is_stopword(*term)) {
                parsed_query.push_back(*term);
            } else {
                spdlog::warn("Term `{}` is a stopword and will be ignored", raw_term);
            }
        } else {
            spdlog::warn("Term `{}` not found and will be ignored", raw_term);
        }
    }
    return {std::move(id), std::move(parsed_query), {}};
}

auto parse_query_ids(std::string const& query_string) -> Query
{
    auto [id, raw_query] = split_query_at_colon(query_string);
    std::vector<term_id_type> parsed_query;
    std::vector<std::string> term_ids;
    boost::split(term_ids, raw_query, boost::is_any_of("\t, ,\v,\f,\r,\n"));

    auto is_empty = [](const std::string& val) { return val.empty(); };
    // remove_if move matching elements to the end, preparing them for erase.
    term_ids.erase(std::remove_if(term_ids.begin(), term_ids.end(), is_empty), term_ids.end());

    try {
        auto to_int = [](const std::string& val) { return std::stoi(val); };
        std::transform(term_ids.begin(), term_ids.end(), std::back_inserter(parsed_query), to_int);
    } catch (std::invalid_argument& err) {
        spdlog::error("Could not parse term identifiers of query `{}`", raw_query);
        exit(1);
    }
    return {std::move(id), std::move(parsed_query), {}};
}

std::function<void(const std::string)> resolve_query_parser(
    std::vector<Query>& queries,
    std::unique_ptr<pisa::Tokenizer> tokenizer,
    std::optional<std::string> const& terms_file,
    std::optional<std::string> const& stopwords_filename,
    std::optional<std::string> const& stemmer_type)
{
    if (terms_file) {
        auto term_processor = TermProcessor(terms_file, stopwords_filename, stemmer_type);
        return [&queries,
                tokenizer = std::shared_ptr<Tokenizer>(std::move(tokenizer)),
                term_processor = std::move(term_processor)](std::string const& query_line) {
            queries.push_back(parse_query_terms(query_line, *tokenizer, term_processor));
        };
    }
    return [&queries](std::string const& query_line) {
        queries.push_back(parse_query_ids(query_line));
    };
}

bool read_query(term_id_vec& ret, std::istream& is)
{
    ret.clear();
    std::string line;
    if (!std::getline(is, line)) {
        return false;
    }
    ret = parse_query_ids(line).terms;
    return true;
}

void remove_duplicate_terms(term_id_vec& terms)
{
    std::sort(terms.begin(), terms.end());
    terms.erase(std::unique(terms.begin(), terms.end()), terms.end());
}

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

}  // namespace pisa
