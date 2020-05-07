#pragma once

#include <functional>
#include <istream>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <gsl/span>
#include <nlohmann/json.hpp>

namespace pisa {

namespace query {
    constexpr std::size_t unlimited = std::numeric_limits<std::size_t>::max();
}

using TermId = std::uint32_t;

struct QueryContainerInner;

struct ResolvedTerm {
    std::uint32_t id;
    std::string term;
};

using TermProcessorFn = std::function<std::optional<std::string>(std::string)>;
using ParseFn = std::function<std::vector<ResolvedTerm>(std::string const&)>;

class QueryContainer;

enum class RequestFlag : std::uint32_t {
    Threshold = 0b01,
    Weights = 0b10,
};

struct RequestFlagSet {
    std::uint32_t flags;
    [[nodiscard]] static constexpr auto all() -> RequestFlagSet { return RequestFlagSet{0b11}; }
    void remove(RequestFlag flag);
    [[nodiscard]] auto operator^(RequestFlag flag) -> RequestFlagSet;
    [[nodiscard]] auto contains(RequestFlag flag) -> bool;
};

[[nodiscard]] auto operator|(RequestFlag lhs, RequestFlag rhs) -> RequestFlagSet;
[[nodiscard]] auto operator&(RequestFlag lhs, RequestFlag rhs) -> RequestFlagSet;
[[nodiscard]] auto operator|(RequestFlagSet lhs, RequestFlag rhs) -> RequestFlagSet;
[[nodiscard]] auto operator&(RequestFlagSet lhs, RequestFlag rhs) -> RequestFlagSet;
auto operator|=(RequestFlagSet& lhs, RequestFlag rhs) -> RequestFlagSet&;
auto operator&=(RequestFlagSet& lhs, RequestFlag rhs) -> RequestFlagSet&;

/// QueryRequest is a special container that maintains important invariants, such as sorted term
/// IDs, and also has some additional data, like term weights, etc.
class QueryRequest {
  public:
    explicit QueryRequest(QueryContainer const& data, std::size_t k, RequestFlagSet flags);

    [[nodiscard]] auto term_ids() const -> gsl::span<std::uint32_t const>;
    [[nodiscard]] auto term_weights() const -> gsl::span<float const>;
    [[nodiscard]] auto threshold() const -> std::optional<float>;
    [[nodiscard]] auto k() const -> std::size_t;

  private:
    std::size_t m_k;
    std::optional<float> m_threshold{};
    std::vector<std::uint32_t> m_term_ids{};
    std::vector<float> m_term_weights{};
};

class QueryContainer {
  public:
    QueryContainer(QueryContainer const&);
    QueryContainer(QueryContainer&&) noexcept;
    QueryContainer& operator=(QueryContainer const&);
    QueryContainer& operator=(QueryContainer&&) noexcept;
    ~QueryContainer();

    [[nodiscard]] auto operator==(QueryContainer const& other) const noexcept -> bool;

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

    /// Constructs a query from a JSON object.
    [[nodiscard]] static auto from_json(std::string_view json_string) -> QueryContainer;

    [[nodiscard]] auto to_json_string() const -> std::string;
    [[nodiscard]] auto to_json() const -> nlohmann::json;

    /// Constructs a query from a colon-separated format:
    ///
    /// ```
    /// id:raw query string
    /// ```
    /// or
    /// ```
    /// raw query string
    /// ```
    [[nodiscard]] static auto from_colon_format(std::string_view line) -> QueryContainer;

    // Accessors

    [[nodiscard]] auto id() const noexcept -> std::optional<std::string> const&;
    [[nodiscard]] auto string() const noexcept -> std::optional<std::string> const&;
    [[nodiscard]] auto terms() const noexcept -> std::optional<std::vector<std::string>> const&;
    [[nodiscard]] auto term_ids() const noexcept -> std::optional<std::vector<std::uint32_t>> const&;
    [[nodiscard]] auto threshold(std::size_t k) const noexcept -> std::optional<float>;
    [[nodiscard]] auto thresholds() const noexcept
        -> std::vector<std::pair<std::size_t, float>> const&;

    /// Sets the raw string.
    [[nodiscard]] auto string(std::string) -> QueryContainer&;

    /// Parses the raw query with the given parser.
    ///
    /// \throws std::domain_error   when raw string is not set
    auto parse(ParseFn parse_fn) -> QueryContainer&;

    /// Sets the query score threshold for `k`.
    ///
    /// If another threshold for the same `k` exists, it will be replaced,
    /// and `true` will be returned. Otherwise, `false` will be returned.
    auto add_threshold(std::size_t k, float score) -> bool;

    /// Preserve only terms at given positions.
    void filter_terms(gsl::span<std::size_t const> term_positions);

    /// Returns a query ready to be used for retrieval.
    ///
    /// This function takes `k` and resolves the associated threshold if exists.
    /// For unranked queries, pass `pisa::query::unlimited` explicitly to avoidi mistakes.
    [[nodiscard]] auto query(std::size_t k, RequestFlagSet flags = RequestFlagSet::all()) const
        -> QueryRequest;

  private:
    QueryContainer();
    std::unique_ptr<QueryContainerInner> m_data;
};

enum class Format { Json, Colon };

class QueryReader {
  public:
    using map_function_type = std::function<QueryContainer(QueryContainer)>;
    using filter_function_type = std::function<bool(QueryContainer const&)>;

    /// Open reader from file.
    static auto from_file(std::string const& file) -> QueryReader;
    /// Open reader from stdin.
    static auto from_stdin() -> QueryReader;

    /// Read next query or return `nullopt` if stream has ended.
    [[nodiscard]] auto next() -> std::optional<QueryContainer>;

    /// Execute `fn(q)` for each query `q`.
    template <typename Fn>
    void for_each(Fn&& fn)
    {
        auto query = next();
        while (query) {
            fn(std::move(*query));
            query = next();
        }
    }

    [[nodiscard]] auto map(map_function_type fn) && -> QueryReader;
    [[nodiscard]] auto filter(filter_function_type fn) && -> QueryReader;

  private:
    explicit QueryReader(std::unique_ptr<std::istream> stream, std::istream& stream_ref);
    static auto next_query(QueryReader& reader) -> std::optional<QueryContainer>;

    std::unique_ptr<std::istream> m_stream;
    std::istream& m_stream_ref;
    std::string m_line_buf{};
    std::optional<Format> m_format{};
    std::vector<map_function_type> m_map_functions{};
    std::vector<filter_function_type> m_filter_functions{};
};

}  // namespace pisa
