#pragma once

#include <functional>
#include <optional>
#include <string>

#include "query.hpp"

namespace pisa {

using TermResolver = std::function<std::optional<ResolvedTerm>(std::string)>;

struct StandardTermResolverParams;

/// Provides a standard implementation of `TermResolver`.
class StandardTermResolver {
  public:
    StandardTermResolver(
        std::string const& term_lexicon_path,
        std::optional<std::string> const& stopwords_filename,
        std::optional<std::string> const& stemmer_type);
    StandardTermResolver(StandardTermResolver const&);
    StandardTermResolver(StandardTermResolver&&) noexcept;
    StandardTermResolver& operator=(StandardTermResolver const&);
    StandardTermResolver& operator=(StandardTermResolver&&) noexcept;
    ~StandardTermResolver();

    [[nodiscard]] auto operator()(std::string token) const -> std::optional<ResolvedTerm>;

  private:
    [[nodiscard]] auto is_stopword(std::uint32_t const term) const -> bool;

    std::unique_ptr<StandardTermResolverParams> m_self;
};

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
