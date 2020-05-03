#pragma once

#include <functional>
#include <istream>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <gsl/span>

namespace pisa {

// using DocId = std::uint32_t;
// using Frequency = std::uint32_t;
// using Score = float;
using TermId = std::uint32_t;

struct QueryContainerInner;

struct ResolvedTerm {
    std::uint32_t id;
    std::string term;
};

using TermProcessorFn = std::function<std::optional<std::string>(std::string)>;
using ParseFn = std::function<std::vector<ResolvedTerm>(std::string const&)>;

class QueryContainer;

/// QueryRequest is a special container that maintains important invariants, such as sorted term
/// IDs, and also has some additional data, like term weights, etc.
class QueryRequest {
  public:
    explicit QueryRequest(QueryContainer const& data, std::size_t k);

    [[nodiscard]] auto term_ids() const -> gsl::span<std::uint32_t const>;
    [[nodiscard]] auto term_weights() const -> gsl::span<float const>;
    [[nodiscard]] auto threshold() const -> std::optional<float>;
    [[nodiscard]] auto k() const -> std::optional<float>;

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

    [[nodiscard]] auto to_json() const -> std::string;

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

    /// Returns a query ready to be used for retrieval.
    [[nodiscard]] auto query(std::size_t k) const -> QueryRequest;

  private:
    QueryContainer();
    std::unique_ptr<QueryContainerInner> m_data;
};

enum class Format { Json, Colon };

class QueryReader {
  public:
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

  private:
    explicit QueryReader(std::unique_ptr<std::istream> stream, std::istream& stream_ref);

    std::unique_ptr<std::istream> m_stream;
    std::istream& m_stream_ref;
    std::string m_line_buf{};
    std::optional<Format> m_format{};
};

/// Eliminates duplicates in a sorted sequence, and returns a vector of counts.
template <class ForwardIt>
[[nodiscard]] auto unique_with_counts(ForwardIt first, ForwardIt last) -> std::vector<std::size_t>
{
    std::vector<std::size_t> counts;

    if (first == last) {
        return counts;
    }

    ForwardIt result = first;
    while (++first != last) {
        if (!(*result == *first) && ++result != first) {
            *result = std::move(*first);
            counts.back() += 1;
        } else {
            counts.push_back(1);
        }
    }
    return counts;
}

}  // namespace pisa
