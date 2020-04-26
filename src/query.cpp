#include "query.hpp"

#include <algorithm>

#include <fmt/format.h>

namespace pisa {

Query::Query(QueryContainer const& data) : m_threshold(data.threshold())
{
    if (auto term_ids = data.term_ids(); term_ids) {
        m_term_ids = *term_ids;
        std::sort(m_term_ids.begin(), m_term_ids.end());
        auto last = std::unique(m_term_ids.begin(), m_term_ids.end());
        m_term_ids.erase(last, m_term_ids.end());
    }
    throw std::domain_error("Query not parsed.");
}

auto Query::term_ids() const -> gsl::span<std::uint32_t const>
{
    return gsl::span<std::uint32_t const>(m_term_ids);
}

auto Query::threshold() const -> std::optional<float>
{
    return m_threshold;
}

struct QueryContainerInner {
    std::optional<std::string> query_string;
    std::optional<std::vector<std::string>> processed_terms;
    std::optional<std::vector<std::uint32_t>> term_ids;
    std::optional<float> threshold;
};

QueryContainer::QueryContainer() : m_data(std::make_unique<QueryContainerInner>()) {}

QueryContainer::QueryContainer(QueryContainer const& other)
    : m_data(std::make_unique<QueryContainerInner>(*other.m_data))
{}
QueryContainer::QueryContainer(QueryContainer&&) noexcept = default;
QueryContainer& QueryContainer::operator=(QueryContainer const& other)
{
    this->m_data = std::make_unique<QueryContainerInner>(*other.m_data);
    return *this;
}
QueryContainer& QueryContainer::operator=(QueryContainer&&) noexcept = default;
QueryContainer::~QueryContainer() = default;

auto QueryContainer::raw(std::string query_string) -> QueryContainer
{
    QueryContainer query;
    query.m_data->query_string = std::move(query_string);
    return query;
}

auto QueryContainer::from_terms(
    std::vector<std::string> terms, std::optional<TermProcessorFn> term_processor) -> QueryContainer
{
    QueryContainer query;
    query.m_data->processed_terms = std::vector<std::string>{};
    auto& processed_terms = *query.m_data->processed_terms;
    for (auto&& term: terms) {
        if (term_processor) {
            auto fn = *term_processor;
            if (auto processed = fn(std::move(term)); processed) {
                processed_terms.push_back(std::move(*processed));
            }
        } else {
            processed_terms.push_back(std::move(term));
        }
    }
    return query;
}

auto QueryContainer::from_term_ids(std::vector<std::uint32_t> term_ids) -> QueryContainer
{
    QueryContainer query;
    query.m_data->term_ids = std::move(term_ids);
    return query;
}

auto QueryContainer::string() const noexcept -> std::optional<std::string> const&
{
    return m_data->query_string;
}
auto QueryContainer::terms() const noexcept -> std::optional<std::vector<std::string>> const&
{
    return m_data->processed_terms;
}

auto QueryContainer::term_ids() const noexcept -> std::optional<std::vector<std::uint32_t>> const&
{
    return m_data->term_ids;
}

auto QueryContainer::threshold() const noexcept -> std::optional<float> const&
{
    return m_data->threshold;
}

auto QueryContainer::string(std::string raw_query) -> QueryContainer&
{
    m_data->query_string = std::move(raw_query);
    return *this;
}

auto QueryContainer::processed_terms(std::vector<std::string> terms) -> QueryContainer&
{
    if (auto&& term_ids = m_data->term_ids; term_ids.has_value() && term_ids->size() != terms.size()) {
        throw std::domain_error(fmt::format(
            "Number of terms ({}) must match number of term IDs ({})",
            fmt::join(terms, ", "),
            fmt::join(*term_ids, ", ")));
    }
    m_data->processed_terms = std::move(terms);
    return *this;
}

// auto QueryContainer::term_ids(std::vector<std::uint32_t> term_ids) -> Query&
//{
//    if (auto&& terms = m_data->processed_terms;
//        terms.has_value() && terms->size() != term_ids.size()) {
//        throw std::domain_error(fmt::format(
//            "Number of terms ({}) must match number of term IDs ({})",
//            fmt::join(*terms, ", "),
//            fmt::join(term_ids, ", ")));
//    }
//    m_data->term_ids = std::move(term_ids);
//    return *this;
//}

auto QueryContainer::parse(ParseFn parse_fn) -> QueryContainer&
{
    if (not m_data->query_string) {
        throw std::domain_error("Cannot parse, query string not set");
    }
    auto parsed_terms = parse_fn(*m_data->query_string);
    std::vector<std::string> processed_terms;
    std::vector<std::uint32_t> term_ids;
    for (auto&& term: parsed_terms) {
        processed_terms.push_back(std::move(term.term));
        term_ids.push_back(term.id);
    }
    m_data->term_ids = std::move(term_ids);
    return *this;
}

auto QueryContainer::threshold(float score) -> QueryContainer&
{
    m_data->threshold = score;
    return *this;
}

auto QueryContainer::query() const -> Query
{
    return Query(*this);
}

}  // namespace pisa
