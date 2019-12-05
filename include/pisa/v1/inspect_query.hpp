#pragma once

#include <functional>

#include <fmt/format.h>
#include <gsl/span>

namespace pisa::v1 {

struct Query;

struct QueryInspector {

    template <typename R>
    explicit constexpr QueryInspector(R writer)
        : m_internal_analyzer(std::make_unique<InspectorImpl<R>>(writer))
    {
    }
    QueryInspector() = default;
    QueryInspector(QueryInspector const& other)
        : m_internal_analyzer(other.m_internal_analyzer->clone())
    {
    }
    QueryInspector(QueryInspector&& other) noexcept = default;
    QueryInspector& operator=(QueryInspector const& other) = delete;
    QueryInspector& operator=(QueryInspector&& other) noexcept = default;
    ~QueryInspector() = default;

    void operator()(Query const& query) { m_internal_analyzer->operator()(query); }
    void summarize() && { std::move(*m_internal_analyzer).summarize(); }

    struct InspectorInterface {
        InspectorInterface() = default;
        InspectorInterface(InspectorInterface const&) = default;
        InspectorInterface(InspectorInterface&&) noexcept = default;
        InspectorInterface& operator=(InspectorInterface const&) = default;
        InspectorInterface& operator=(InspectorInterface&&) noexcept = default;
        virtual ~InspectorInterface() = default;
        virtual void operator()(Query const& query) = 0;
        virtual void summarize() && = 0;
        [[nodiscard]] virtual auto clone() const -> std::unique_ptr<InspectorInterface> = 0;
    };

    template <typename R>
    struct InspectorImpl : InspectorInterface {
        explicit InspectorImpl(R analyzer) : m_analyzer(std::move(analyzer)) {}
        InspectorImpl() = default;
        InspectorImpl(InspectorImpl const&) = default;
        InspectorImpl(InspectorImpl&&) noexcept = default;
        InspectorImpl& operator=(InspectorImpl const&) = default;
        InspectorImpl& operator=(InspectorImpl&&) noexcept = default;
        ~InspectorImpl() override = default;
        void operator()(Query const& query) override { m_analyzer(query); }
        void summarize() && override { std::move(m_analyzer).summarize(); }
        [[nodiscard]] auto clone() const -> std::unique_ptr<InspectorInterface> override
        {
            auto copy = *this;
            return std::make_unique<InspectorImpl<R>>(std::move(copy));
        }

       private:
        R m_analyzer;
    };

   private:
    std::unique_ptr<InspectorInterface> m_internal_analyzer;
};

} // namespace pisa::v1
