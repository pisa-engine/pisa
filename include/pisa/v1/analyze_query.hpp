#pragma once

#include <functional>

#include <fmt/format.h>
#include <gsl/span>

namespace pisa::v1 {

struct Query;

struct QueryAnalyzer {

    template <typename R>
    explicit constexpr QueryAnalyzer(R writer)
        : m_internal_analyzer(std::make_unique<AnalyzerImpl<R>>(writer))
    {
    }
    QueryAnalyzer() = default;
    QueryAnalyzer(QueryAnalyzer const& other)
        : m_internal_analyzer(other.m_internal_analyzer->clone())
    {
    }
    QueryAnalyzer(QueryAnalyzer&& other) noexcept = default;
    QueryAnalyzer& operator=(QueryAnalyzer const& other) = delete;
    QueryAnalyzer& operator=(QueryAnalyzer&& other) noexcept = default;
    ~QueryAnalyzer() = default;

    void operator()(Query const& query) { m_internal_analyzer->operator()(query); }
    void summarize() && { std::move(*m_internal_analyzer).summarize(); }

    struct AnalyzerInterface {
        AnalyzerInterface() = default;
        AnalyzerInterface(AnalyzerInterface const&) = default;
        AnalyzerInterface(AnalyzerInterface&&) noexcept = default;
        AnalyzerInterface& operator=(AnalyzerInterface const&) = default;
        AnalyzerInterface& operator=(AnalyzerInterface&&) noexcept = default;
        virtual ~AnalyzerInterface() = default;
        virtual void operator()(Query const& query) = 0;
        virtual void summarize() && = 0;
        [[nodiscard]] virtual auto clone() const -> std::unique_ptr<AnalyzerInterface> = 0;
    };

    template <typename R>
    struct AnalyzerImpl : AnalyzerInterface {
        explicit AnalyzerImpl(R analyzer) : m_analyzer(std::move(analyzer)) {}
        AnalyzerImpl() = default;
        AnalyzerImpl(AnalyzerImpl const&) = default;
        AnalyzerImpl(AnalyzerImpl&&) noexcept = default;
        AnalyzerImpl& operator=(AnalyzerImpl const&) = default;
        AnalyzerImpl& operator=(AnalyzerImpl&&) noexcept = default;
        ~AnalyzerImpl() override = default;
        void operator()(Query const& query) override { m_analyzer(query); }
        void summarize() && override { std::move(m_analyzer).summarize(); }
        [[nodiscard]] auto clone() const -> std::unique_ptr<AnalyzerInterface> override
        {
            auto copy = *this;
            return std::make_unique<AnalyzerImpl<R>>(std::move(copy));
        }

       private:
        R m_analyzer;
    };

   private:
    std::unique_ptr<AnalyzerInterface> m_internal_analyzer;
};

} // namespace pisa::v1
