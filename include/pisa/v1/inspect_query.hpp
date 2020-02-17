#pragma once

#include <functional>
#include <string_view>

#include <fmt/format.h>
#include <gsl/span>

#include "topk_queue.hpp"
#include "v1/query.hpp"

namespace pisa::v1 {

template <typename FirstValue, typename... Values>
auto write_delimited(std::ostream& os,
                     std::string_view sep,
                     FirstValue first_value,
                     Values&&... values) -> std::ostream&;

template <typename Value>
auto write_delimited(std::ostream& os, [[maybe_unused]] std::string_view sep, Value value)
    -> std::ostream&
{
    return os << value;
}

template <typename... T>
auto write_delimited(std::ostream& os, std::string_view sep, std::tuple<T...> t) -> std::ostream&
{
    std::apply([&](auto... vals) { write_delimited(os, sep, vals...); }, t);
    return os;
}

template <typename T1, typename T2>
auto write_delimited(std::ostream& os, std::string_view sep, std::pair<T1, T2> p) -> std::ostream&
{
    write_delimited(os, sep, p.first);
    os << sep;
    return write_delimited(os, sep, p.second);
}

/// Writes a list of values into a stream separated by `sep`.
template <typename FirstValue, typename... Values>
auto write_delimited(std::ostream& os,
                     std::string_view sep,
                     FirstValue first_value,
                     Values&&... values) -> std::ostream&
{
    write_delimited(os, sep, first_value);
    (
        [&] {
            os << sep;
            write_delimited(os, sep, values);
        }(),
        ...);
    return os;
}

struct InspectCount {
    using value_type = std::size_t;
    void reset() { m_current_count = 0; }
    void inc(std::size_t n = 1)
    {
        m_current_count += n;
        m_total_count += n;
    }
    [[nodiscard]] auto get() const -> std::size_t { return m_current_count; }
    [[nodiscard]] auto mean(std::size_t n) const -> float
    {
        return static_cast<float>(m_total_count) / n;
    }

    struct Result {
        explicit Result(std::size_t value) : m_value(value) {}
        [[nodiscard]] auto get() const { return m_value; }
        [[nodiscard]] auto operator+(Result const& other) const { return m_value + other.m_value; }

       private:
        std::size_t m_value;
    };

   private:
    std::size_t m_current_count = 0;
    std::size_t m_total_count = 0;
};

struct InspectPostings : InspectCount {
    struct Result : InspectCount::Result {
        explicit Result(std::size_t value) : InspectCount::Result(value) {}
        [[nodiscard]] auto postings() const { return get(); }
    };
    void posting() { inc(); }
    static auto header(std::string_view suffix) { return fmt::format("postings{}", suffix); }
};

struct InspectDocuments : InspectCount {
    struct Result : InspectCount::Result {
        explicit Result(std::size_t value) : InspectCount::Result(value) {}
        [[nodiscard]] auto documents() const { return get(); }
    };
    void document() { inc(); }
    static auto header(std::string_view suffix) { return fmt::format("documents{}", suffix); }
};

struct InspectLookups : InspectCount {
    struct Result : InspectCount::Result {
        explicit Result(std::size_t value) : InspectCount::Result(value) {}
        [[nodiscard]] auto lookups() const { return get(); }
    };
    void lookup() { inc(); }
    static auto header(std::string_view suffix) { return fmt::format("lookups{}", suffix); }
};

struct InspectInserts : InspectCount {
    struct Result : InspectCount::Result {
        explicit Result(std::size_t value) : InspectCount::Result(value) {}
        [[nodiscard]] auto inserts() const { return get(); }
    };
    void insert() { inc(); }
    static auto header(std::string_view suffix) { return fmt::format("inserts{}", suffix); }
};

struct InspectEssential : InspectCount {
    struct Result : InspectCount::Result {
        explicit Result(std::size_t value) : InspectCount::Result(value) {}
        [[nodiscard]] auto essentials() const { return get(); }
    };
    void essential(std::size_t n) { inc(n); }
    static auto header(std::string_view suffix) { return fmt::format("essential-terms{}", suffix); }
};

template <typename Inspect>
struct InspectPartitioned {
    struct Result {
        explicit Result(typename Inspect::Result first, typename Inspect::Result second)
            : first(first), second(second), sum(first + second)
        {
        }
        [[nodiscard]] auto get() const
        {
            return std::make_tuple(sum.get(), first.get(), second.get());
        }

        typename Inspect::Result first;
        typename Inspect::Result second;
        typename Inspect::Result sum;
    };
    using value_type = Result;

    void reset()
    {
        m_components.first.reset();
        m_components.second.reset();
    }
    void inc(std::size_t n = 1)
    {
        m_components.first.inc(n);
        m_components.second.inc(n);
    }
    [[nodiscard]] auto get() const
    {
        return Result(m_components.first.get(), m_components.second.get());
    }
    [[nodiscard]] auto mean(std::size_t n) const
    {
        return Result(m_components.first.mean(n), m_components.second.mean(n));
    }
    [[nodiscard]] auto first() -> Inspect* { return &m_components.first; }
    [[nodiscard]] auto second() -> Inspect* { return &m_components.second; }
    static auto header(std::string_view suffix)
    {
        return std::make_tuple(Inspect::header(fmt::format("{}", suffix)),
                               Inspect::header(fmt::format("{}_1", suffix)),
                               Inspect::header(fmt::format("{}_2", suffix)));
    }

   private:
    std::pair<Inspect, Inspect> m_components;
};

template <typename First, typename Second>
struct InspectPair {
    struct Result {
        explicit Result(typename First::Result first, typename Second::Result second)
            : first(first), second(second)
        {
        }
        [[nodiscard]] auto get() const { return std::make_pair(first.get(), second.get()); }

        typename First::Result first;
        typename Second::Result second;
    };
    using value_type = Result;

    void reset()
    {
        m_components.first.reset();
        m_components.second.reset();
    }
    void inc(std::size_t n = 1)
    {
        m_components.first.inc(n);
        m_components.second.inc(n);
    }
    [[nodiscard]] auto get() const
    {
        return Result(m_components.first.get(), m_components.second.get());
    }
    [[nodiscard]] auto mean(std::size_t n) const
    {
        return Result(m_components.first.mean(n), m_components.second.mean(n));
    }
    [[nodiscard]] auto first() -> First* { return &m_components.first; }
    [[nodiscard]] auto second() -> Second* { return &m_components.second; }
    static auto header(std::string_view suffix)
    {
        return std::make_pair(First::header(fmt::format("{}_1", suffix)),
                              Second::header(fmt::format("{}_2", suffix)));
    }

   private:
    std::pair<First, Second> m_components;
};

template <typename... Stat>
struct InspectMany : Stat... {
    struct Result : Stat::Result... {
        explicit Result(typename Stat::value_type... values) : Stat::Result(values)... {}
        Result(Result const& result) = default;
        Result(Result&& result) noexcept = default;
        Result& operator=(Result const& result) = default;
        Result& operator=(Result&& result) noexcept = default;
        ~Result() = default;
        [[nodiscard]] auto get() const { return std::make_tuple(Stat::Result::get()...); }
        [[nodiscard]] auto operator+(Result const& other) const
        {
            return Result((Stat::Result::get() + other.Stat::Result::get())...);
        }
    };
    using value_type = Result;
    void reset() { (Stat::reset(), ...); }
    void inc(std::size_t n = 1) { (Stat::inc(n), ...); }
    [[nodiscard]] auto get() const { return Result(Stat::get()...); }
    [[nodiscard]] auto mean(std::size_t n) const { return Result(Stat::mean(n)...); }
    static auto header(std::string_view suffix) { return std::make_tuple(Stat::header(suffix)...); }
};

template <typename Index, typename Scorer, typename... Stat>
struct Inspect : Stat... {
    Inspect(Index const& index, Scorer scorer) : m_index(index), m_scorer(std::move(scorer)) {}

    virtual void run(Query const& query,
                     Index const& index,
                     Scorer const& scorer,
                     topk_queue topk) = 0;

    [[nodiscard]] auto mean() const { return InspectResult(Stat::mean(m_count)...); }

    struct InspectResult : Stat::Result... {
        explicit InspectResult(typename Stat::value_type... values) : Stat::Result(values)... {}
        std::ostream& write(std::ostream& os, std::string_view sep = "\t")
        {
            return write_delimited(os, sep, Stat::Result::get()...);
        }
    };

    [[nodiscard]] auto operator()(Query const& query) -> InspectResult
    {
        (Stat::reset(), ...);
        run(query, m_index, m_scorer, topk_queue(query.k()));
        m_count += 1;
        return InspectResult(Stat::get()...);
    }
    static std::ostream& header(std::ostream& os, std::string_view sep = "\t")
    {
        return write_delimited(os, sep, Stat::header("")...);
    }

   private:
    std::size_t m_count = 0;
    Index const& m_index;
    Scorer m_scorer;
};

struct InspectResult {

    template <typename R>
    explicit constexpr InspectResult(R result) : m_inner(std::make_unique<ResultImpl<R>>(result))
    {
    }
    InspectResult() = default;
    InspectResult(InspectResult const& other) : m_inner(other.m_inner->clone()) {}
    InspectResult(InspectResult&& other) noexcept = default;
    InspectResult& operator=(InspectResult const& other) = delete;
    InspectResult& operator=(InspectResult&& other) noexcept = default;
    ~InspectResult() = default;

    std::ostream& write(std::ostream& os, std::string_view sep = "\t")
    {
        return m_inner->write(os, sep);
    }

    struct ResultInterface {
        ResultInterface() = default;
        ResultInterface(ResultInterface const&) = default;
        ResultInterface(ResultInterface&&) noexcept = default;
        ResultInterface& operator=(ResultInterface const&) = default;
        ResultInterface& operator=(ResultInterface&&) noexcept = default;
        virtual ~ResultInterface() = default;
        virtual std::ostream& write(std::ostream& os, std::string_view sep) = 0;
        [[nodiscard]] virtual auto clone() const -> std::unique_ptr<ResultInterface> = 0;
    };

    template <typename R>
    struct ResultImpl : ResultInterface {
        explicit ResultImpl(R result) : m_result(std::move(result)) {}
        ResultImpl() = default;
        ResultImpl(ResultImpl const&) = default;
        ResultImpl(ResultImpl&&) noexcept = default;
        ResultImpl& operator=(ResultImpl const&) = default;
        ResultImpl& operator=(ResultImpl&&) noexcept = default;
        ~ResultImpl() override = default;
        std::ostream& write(std::ostream& os, std::string_view sep) override
        {
            return m_result.write(os, sep);
        }
        [[nodiscard]] auto clone() const -> std::unique_ptr<ResultInterface> override
        {
            auto copy = *this;
            return std::make_unique<ResultImpl<R>>(std::move(copy));
        }

       private:
        R m_result;
    };

   private:
    std::unique_ptr<ResultInterface> m_inner;
};

struct QueryInspector {

    template <typename R>
    explicit constexpr QueryInspector(R writer)
        : m_inner(std::make_unique<InspectorImpl<R>>(writer))
    {
    }
    QueryInspector() = default;
    QueryInspector(QueryInspector const& other) : m_inner(other.m_inner->clone()) {}
    QueryInspector(QueryInspector&& other) noexcept = default;
    QueryInspector& operator=(QueryInspector const& other) = delete;
    QueryInspector& operator=(QueryInspector&& other) noexcept = default;
    ~QueryInspector() = default;

    InspectResult operator()(Query const& query) { return m_inner->operator()(query); }
    InspectResult mean() { return m_inner->mean(); }
    std::ostream& header(std::ostream& os) { return m_inner->header(os); }

    struct InspectorInterface {
        InspectorInterface() = default;
        InspectorInterface(InspectorInterface const&) = default;
        InspectorInterface(InspectorInterface&&) noexcept = default;
        InspectorInterface& operator=(InspectorInterface const&) = default;
        InspectorInterface& operator=(InspectorInterface&&) noexcept = default;
        virtual ~InspectorInterface() = default;
        virtual InspectResult operator()(Query const& query) = 0;
        virtual InspectResult mean() = 0;
        virtual std::ostream& header(std::ostream& os) = 0;
        [[nodiscard]] virtual auto clone() const -> std::unique_ptr<InspectorInterface> = 0;
    };

    template <typename R>
    struct InspectorImpl : InspectorInterface {
        explicit InspectorImpl(R inspect) : m_inspect(std::move(inspect)) {}
        InspectorImpl() = default;
        InspectorImpl(InspectorImpl const&) = default;
        InspectorImpl(InspectorImpl&&) noexcept = default;
        InspectorImpl& operator=(InspectorImpl const&) = default;
        InspectorImpl& operator=(InspectorImpl&&) noexcept = default;
        ~InspectorImpl() override = default;
        InspectResult operator()(Query const& query) override
        {
            return InspectResult(m_inspect(query));
        }
        InspectResult mean() override { return InspectResult(m_inspect.mean()); }
        [[nodiscard]] auto clone() const -> std::unique_ptr<InspectorInterface> override
        {
            auto copy = *this;
            return std::make_unique<InspectorImpl<R>>(std::move(copy));
        }
        std::ostream& header(std::ostream& os) override { return m_inspect.header(os); }

       private:
        R m_inspect;
    };

   private:
    std::unique_ptr<InspectorInterface> m_inner;
};

} // namespace pisa::v1
