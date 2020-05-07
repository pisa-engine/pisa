#pragma once

#include <string>

#include "query.hpp"
#include "term_resolver.hpp"

namespace pisa {

/// Parses a query string to processed terms.
class QueryParser {
  public:
    explicit QueryParser(TermResolver term_processor);
    /// Given a query string, it returns a list of (possibly processed) terms.
    ///
    /// Possible transformations of terms include lower-casing and stemming.
    /// Some terms could be also removed, e.g., because they are on a list of
    /// stop words. The exact implementation depends on the term processor
    /// passed to the constructor.
    auto operator()(std::string const&) -> std::vector<ResolvedTerm>;

  private:
    TermResolver m_term_resolver;
};

}  // namespace pisa
