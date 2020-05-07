#pragma once

#include <functional>
#include <optional>
#include <string>

#include "payload_vector.hpp"
#include "query.hpp"

namespace pisa {

/// Thrown if expected resolver but none found.
struct MissingResolverError {
};

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

/// Reads queries from `query_file`, resolves them with `term_resolver`, filters by
/// query length (number of resolved terms in the query), and prints the selected
/// queries to `out`.
///
/// \throws MissingResolverError  When no resolver passed but queries don't have IDs resolved.
//
void filter_queries(
    std::optional<std::string> const& query_file,
    std::optional<TermResolver> term_resolver,
    std::size_t min_query_len,
    std::size_t max_query_len,
    std::ostream& out);

}  // namespace pisa
