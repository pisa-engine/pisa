#pragma once

#include "query/queries.hpp"
#include "term_map.hpp"
#include "text_analyzer.hpp"

namespace pisa {

class QueryParser {
    TextAnalyzer m_analyzer;
    std::unique_ptr<TermMap> m_term_map;

  public:
    explicit QueryParser(TextAnalyzer analyzer, std::unique_ptr<TermMap> term_map = nullptr);
    [[nodiscard]] auto parse(std::string_view query) -> Query;
    [[nodiscard]] auto parse(std::string const& query) -> Query;
};

}  // namespace pisa
