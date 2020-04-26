#include "query.hpp"

#include <algorithm>

#include <fmt/format.h>
#include <nlohmann/json.hpp>

namespace pisa {

QueryRequest::QueryRequest(QueryContainer const& data) : m_threshold(data.threshold())
{
    if (auto term_ids = data.term_ids(); term_ids) {
        m_term_ids = *term_ids;
        std::sort(m_term_ids.begin(), m_term_ids.end());
        auto last = std::unique(m_term_ids.begin(), m_term_ids.end());
        m_term_ids.erase(last, m_term_ids.end());
    }
    throw std::domain_error("Query not parsed.");
}

auto QueryRequest::term_ids() const -> gsl::span<std::uint32_t const>
{
    return gsl::span<std::uint32_t const>(m_term_ids);
}

auto QueryRequest::threshold() const -> std::optional<float>
{
    return m_threshold;
}

struct QueryContainerInner {
    std::optional<std::string> id;
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

auto QueryContainer::id() const noexcept -> std::optional<std::string> const&
{
    return m_data->id;
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

auto QueryContainer::query() const -> QueryRequest
{
    return QueryRequest(*this);
}

template <typename T>
[[nodiscard]] auto get(nlohmann::json const& node, std::string_view field) -> std::optional<T>
{
    if (auto pos = node.find(field); pos != node.end()) {
        try {
            return std::make_optional(pos->get<T>());
        } catch (nlohmann::detail::exception const& err) {
            throw std::runtime_error(fmt::format("Requested field {} is of wrong type", field));
        }
    }
    return std::optional<T>{};
}

auto QueryContainer::from_json(std::string_view json_string) -> QueryContainer
{
    try {
        auto json = nlohmann::json::parse(json_string);
        QueryContainer query;
        QueryContainerInner& data = *query.m_data;
        bool at_least_one_required = false;
        if (auto id = get<std::string>(json, "id"); id) {
            data.id = std::move(id);
        }
        if (auto raw = get<std::string>(json, "query"); raw) {
            data.query_string = std::move(raw);
            at_least_one_required = true;
        }
        if (auto terms = get<std::vector<std::string>>(json, "terms"); terms) {
            data.processed_terms = std::move(terms);
            at_least_one_required = true;
        }
        if (auto term_ids = get<std::vector<std::uint32_t>>(json, "term_ids"); term_ids) {
            data.term_ids = std::move(term_ids);
            at_least_one_required = true;
        }
        if (auto threshold = get<float>(json, "threshold"); threshold) {
            data.threshold = threshold;
        }
        if (not at_least_one_required) {
            throw std::invalid_argument(fmt::format(
                "JSON must have either raw query, terms, or term IDs: {}", json_string));
        }
        return query;
    } catch (nlohmann::detail::exception const& err) {
        throw std::runtime_error(
            fmt::format("Failed to parse JSON: `{}`: {}", json_string, err.what()));
    }
}

auto QueryContainer::to_json() const -> std::string
{
    nlohmann::json json;
    if (auto id = m_data->id; id) {
        json["id"] = *id;
    }
    if (auto raw = m_data->query_string; raw) {
        json["query"] = *raw;
    }
    if (auto terms = m_data->processed_terms; terms) {
        json["terms"] = *terms;
    }
    if (auto term_ids = m_data->term_ids; term_ids) {
        json["term_ids"] = *term_ids;
    }
    if (auto threshold = m_data->threshold; threshold) {
        json["threshold"] = *threshold;
    }
    return json.dump();
}

auto QueryContainer::from_colon_format(std::string_view line) -> QueryContainer
{
    auto pos = std::find(line.begin(), line.end(), ':');
    QueryContainer query;
    QueryContainerInner& data = *query.m_data;
    if (pos == line.end()) {
        data.query_string = std::string(line);
    } else {
        data.id = std::string(line.begin(), pos);
        data.query_string = std::string(std::next(pos), line.end());
    }
    return query;
}

}  // namespace pisa
