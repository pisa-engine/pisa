#pragma once

#include "query.hpp"
#include "term_map.hpp"
#include "text_analyzer.hpp"

namespace pisa {

/** Query parser.
 *
 * Parses a string and maps tokens to term IDs.
 */
class QueryParser {
    TextAnalyzer m_analyzer;
    std::unique_ptr<TermMap> m_term_map;

  public:
    /** Constructs a parser.
     *
     * If term map is not passed, then each token will be parsed as a number and treated as
     * term ID.
     */
    explicit QueryParser(TextAnalyzer analyzer, std::unique_ptr<TermMap> term_map);

    /** Constructs a parser with `IntMap`, which parses numbers to term IDs. */
    explicit QueryParser(TextAnalyzer analyzer);

    [[nodiscard]] auto parse(std::string_view query) -> Query;
    [[nodiscard]] auto parse(std::string const& query) -> Query;
};

}  // namespace pisa
