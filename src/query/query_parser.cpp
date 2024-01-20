#include <optional>
#include <string>

#include <spdlog/spdlog.h>

#include "query.hpp"
#include "query/query_parser.hpp"
#include "string.hpp"
#include "term_map.hpp"

namespace pisa {

[[nodiscard]] auto parse_term_id(std::string const& token) -> TermId {
    std::size_t idx;
    TermId tid = std::stol(token, &idx);
    if (idx < token.size()) {
        throw std::invalid_argument("invalid term ID: " + token);
    }
    return tid;
}

QueryParser::QueryParser(TextAnalyzer analyzer, std::unique_ptr<TermMap> term_map)
    : m_analyzer(std::move(analyzer)), m_term_map(std::move(term_map)) {}

QueryParser::QueryParser(TextAnalyzer analyzer)
    : m_analyzer(std::move(analyzer)), m_term_map(std::make_unique<IntMap>()) {}

auto QueryParser::parse(std::string_view query) -> Query {
    auto [qid, raw_query] = split_at_colon(query);
    auto tokens = m_analyzer.analyze(raw_query);
    std::vector<TermId> term_ids;
    for (auto token: *tokens) {
        if (auto tid = m_term_map->find(token); tid) {
            term_ids.push_back(*tid);
        } else {
            spdlog::warn("Term `{}` not found and will be ignored", token);
        }
    }
    return Query(qid ? std::optional<std::string>(*qid) : std::nullopt, term_ids);
}

auto QueryParser::parse(std::string const& query) -> Query {
    return parse(std::string_view(query));
}

}  // namespace pisa
