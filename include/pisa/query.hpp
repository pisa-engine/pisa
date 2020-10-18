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

using TermId = std::uint32_t;

/// Internally, a `std::array` of two terms, but it additionally preserves sorted order of terms.
/// This is an important invariant for serching pairs in dictionary and avoiding having two
/// different pairs that differ only in order their terms appear. Essentially, we want to make it
/// invalid to create a pair with a wrong order of terms.
///
/// To maintain the invariant, only `const` accessors are exposed, and a constructor fixes the
/// order if it is reversed.
class TermPair: private std::array<TermId, 2> {
    using Super = std::array<TermId, 2>;

  public:
    using Super::const_iterator;
    using Super::const_pointer;
    using Super::const_reference;
    using Super::const_reverse_iterator;
    using Super::difference_type;
    using Super::size_type;
    using Super::value_type;

    TermPair() : Super{0, 0} {}
    TermPair(TermId t1, TermId t2) : Super{t1, t2} { std::sort(Super::begin(), Super::end()); }
    TermPair(TermPair const&) = default;
    TermPair(TermPair&&) noexcept = default;
    TermPair& operator=(TermPair const&) = default;
    TermPair& operator=(TermPair&&) noexcept = default;
    ~TermPair() = default;

    explicit TermPair(Super const& other) : Super{other[0], other[1]}
    {
        std::sort(Super::begin(), Super::end());
    }
    explicit TermPair(Super&& other) noexcept : Super{std::get<0>(other), std::get<1>(other)}
    {
        std::sort(Super::begin(), Super::end());
    }
    TermPair& operator=(Super const& other)
    {
        *this = TermPair(other);
        return *this;
    }
    TermPair& operator=(Super&& other) noexcept
    {
        *this = TermPair(other);
        return *this;
    }

    template <std::size_t I>
    constexpr pisa::TermId get() const noexcept
    {
        return std::get<I>(static_cast<std::array<pisa::TermId, 2>>(*this));
    }

    bool operator==(TermPair const& other) const
    {
        return static_cast<std::array<pisa::TermId, 2>>(*this)
            == static_cast<std::array<pisa::TermId, 2>>(other);
    }

    bool operator!=(TermPair const& other) const
    {
        return static_cast<std::array<pisa::TermId, 2>>(*this)
            != static_cast<std::array<pisa::TermId, 2>>(other);
    }

    bool operator<(TermPair const& other) const
    {
        return static_cast<std::array<pisa::TermId, 2>>(*this)
            < static_cast<std::array<pisa::TermId, 2>>(other);
    }

    bool operator<=(TermPair const& other) const
    {
        return static_cast<std::array<pisa::TermId, 2>>(*this)
            <= static_cast<std::array<pisa::TermId, 2>>(other);
    }

    bool operator>(TermPair const& other) const
    {
        return static_cast<std::array<pisa::TermId, 2>>(*this)
            > static_cast<std::array<pisa::TermId, 2>>(other);
    }

    bool operator>=(TermPair const& other) const
    {
        return static_cast<std::array<pisa::TermId, 2>>(*this)
            >= static_cast<std::array<pisa::TermId, 2>>(other);
    }

    constexpr const_iterator begin() const noexcept { return Super::begin(); }
    constexpr const_iterator cbegin() const noexcept { return Super::cbegin(); }
    constexpr const_iterator end() const noexcept { return Super::end(); }
    constexpr const_iterator cend() const noexcept { return Super::cend(); }

    constexpr const_reference at(size_type pos) const { return Super::at(pos); }
    constexpr const_reference front() const { return Super::front(); }
    constexpr const_reference back() const { return Super::back(); }
    // constexpr const TermId* data() const noexcept { return Super::data(); }
    using Super::data;
    void swap(TermPair& other) noexcept { Super::swap(other); }
};

/* bool operator==(TermPair const& lhs, TermPair const& rhs) */
/* { */
/*     return lhs.operator==(rhs); */
/* } */

namespace query {
    constexpr std::size_t unlimited = std::numeric_limits<std::size_t>::max();
}

struct QueryContainerInner;

struct ResolvedTerm {
    std::uint32_t id;
    std::string term;
};

template <typename T>
struct Selection {
    using pair_type = std::conditional_t<std::is_same_v<T, TermId>, TermPair, std::array<T, 2>>;
    std::vector<T> selected_terms;
    std::vector<pair_type> selected_pairs;

    [[nodiscard]] auto operator==(Selection const& other) const noexcept -> bool
    {
        return selected_terms == other.selected_terms && selected_pairs == other.selected_pairs;
    }
};

using TermProcessorFn = std::function<std::optional<std::string>(std::string)>;
using ParseFn = std::function<std::vector<ResolvedTerm>(std::string const&)>;

class QueryContainer;

enum class RequestFlag : std::uint32_t {
    Threshold = 0b001,
    Weights = 0b010,
    Selection = 0b100,
};

struct RequestFlagSet {
    std::uint32_t flags;
    [[nodiscard]] static constexpr auto all() -> RequestFlagSet { return RequestFlagSet{0b111}; }
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
    QueryRequest(QueryContainer const& data, std::size_t k, RequestFlagSet flags);

    [[nodiscard]] auto term_ids() const -> gsl::span<std::uint32_t const>;
    [[nodiscard]] auto term_weights() const -> gsl::span<float const>;
    [[nodiscard]] auto threshold() const -> std::optional<float>;
    [[nodiscard]] auto selection() const -> std::optional<Selection<TermId>>;
    [[nodiscard]] auto k() const -> std::size_t;

  private:
    std::size_t m_k;
    std::optional<float> m_threshold{};
    std::optional<Selection<TermId>> m_selection{};
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

    /// Pretty printed only if `indent >= 0`; by default, one line returned.
    [[nodiscard]] auto to_json_string(int indent = -1) const -> std::string;
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
    [[nodiscard]] auto selection(std::size_t k) const noexcept
        -> std::optional<Selection<std::size_t>>;
    [[nodiscard]] auto selections() const noexcept
        -> std::vector<std::pair<std::size_t, Selection<std::size_t>>> const&;

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

    /// Sets a selection for `k`.
    ///
    /// If another selection for the same `k` exists, it will be replaced,
    /// and `true` will be returned. Otherwise, `false` will be returned.
    auto add_selection(std::size_t k, Selection<std::size_t> selection) -> bool;

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

namespace std {

template <std::size_t I>
constexpr pisa::TermId get(pisa::TermPair const& term_pair) noexcept
{
    return term_pair.get<I>();
}

template <>
struct tuple_size<pisa::TermPair> {
    static constexpr std::size_t value = 2;
};

template <std::size_t I>
struct tuple_element<I, pisa::TermPair> {
    using type = pisa::TermId;
};

}  // namespace std
