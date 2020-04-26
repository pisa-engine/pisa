#pragma once

#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <gsl/span>

namespace pisa {

struct QueryContainerInner;

struct ParsedTerm {
    std::uint32_t id;
    std::string term;
};

using TermProcessorFn = std::function<std::optional<std::string>(std::string)>;
using ParseFn = std::function<std::vector<ParsedTerm>(std::string const&)>;

class QueryContainer;

/// Query is a special container that maintains important invariants, such as sorted term IDs,
/// and also has some additional data, like term weights, etc.
class Query {
  public:
    explicit Query(QueryContainer const& data);

    [[nodiscard]] auto term_ids() const -> gsl::span<std::uint32_t const>;
    [[nodiscard]] auto threshold() const -> std::optional<float>;

  private:
    std::optional<float> m_threshold{};
    std::vector<std::uint32_t> m_term_ids{};
};

class QueryContainer {
  public:
    QueryContainer(QueryContainer const&);
    QueryContainer(QueryContainer&&) noexcept;
    QueryContainer& operator=(QueryContainer const&);
    QueryContainer& operator=(QueryContainer&&) noexcept;
    ~QueryContainer();

    /// Constructs a query from a raw string.
    [[nodiscard]] static auto raw(std::string query_string) -> QueryContainer;

    /// Constructs a query from a list of terms.
    ///
    /// \param terms            List of terms
    /// \param term_processor   Function executed for each term before stroring them,
    ///                         e.g., stemming or filtering. This function returns
    ///                         `std::optional<std::string>`, and all `std::nullopt` values
    ///                         will be filtered out.
    [[nodiscard]] static auto
    from_terms(std::vector<std::string> terms, std::optional<TermProcessorFn> term_processor)
        -> QueryContainer;

    /// Constructs a query from a list of term IDs.
    [[nodiscard]] static auto from_term_ids(std::vector<std::uint32_t> term_ids) -> QueryContainer;

    // Accessors

    [[nodiscard]] auto string() const noexcept -> std::optional<std::string> const&;
    [[nodiscard]] auto terms() const noexcept -> std::optional<std::vector<std::string>> const&;
    [[nodiscard]] auto term_ids() const noexcept -> std::optional<std::vector<std::uint32_t>> const&;
    [[nodiscard]] auto threshold() const noexcept -> std::optional<float> const&;

    /// Sets the raw string.
    [[nodiscard]] auto string(std::string) -> QueryContainer&;

    /// Sets processed terms.
    ///
    /// NOTE: If the intent is to parse the query, use `parse` method instead.
    /// This method is intended to be used when loading a query from JSON or another
    /// external representation.
    ///
    /// \throws std::domain_error   when term IDs are set but the lengths don't match
    auto processed_terms(std::vector<std::string> terms) -> QueryContainer&;

    /// Parses the raw query with the given parser.
    ///
    /// \throws std::domain_error   when raw string is not set
    auto parse(ParseFn parse_fn) -> QueryContainer&;

    /// Sets the query score threshold.
    auto threshold(float score) -> QueryContainer&;

    /// Returns a query ready to be used for retrieval.
    [[nodiscard]] auto query() const -> Query;

  private:
    QueryContainer();
    std::unique_ptr<QueryContainerInner> m_data;
};

}  // namespace pisa
