#include "query/query_parser.hpp"
#include "query/queries.hpp"

namespace pisa {

QueryParser::QueryParser(TextAnalyzer analyzer, std::unique_ptr<TermMap> term_map)
    : m_analyzer(std::move(analyzer)), m_term_map(std::move(term_map))
{}

auto QueryParser::parse(std::string_view query) -> Query
{
    auto [id, raw_query] = split_query_at_colon(query);
    auto tokens = m_analyzer.analyze(raw_query);
    std::vector<std::uint32_t> query_ids;
    for (auto token: *tokens) {
        if (auto id = (*m_term_map)(token); id) {
            query_ids.push_back(*id);
        }
    }
    return {std::move(id), std::move(query_ids), {}};
}

auto QueryParser::parse(std::string const& query) -> Query
{
    return parse(std::string_view(query));
}

}  // namespace pisa
