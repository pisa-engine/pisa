#include "query.hpp"

#include <algorithm>
#include <fstream>
#include <iostream>

#include <fmt/format.h>
#include <nlohmann/json.hpp>

namespace pisa {

[[nodiscard]] auto first_equal_to(std::size_t k)
{
    return [k](auto&& pair) { return pair.first == k; };
}

QueryRequest::QueryRequest(QueryContainer const& data, std::size_t k)
    : m_k(k), m_threshold(data.threshold(k))
{
    if (auto term_ids = data.term_ids(); term_ids) {
        std::map<TermId, std::size_t> counts;
        for (auto term_id: *term_ids) {
            counts[term_id] += 1;
        }
        for (auto [term_id, count]: counts) {
            m_term_ids.push_back(term_id);
            m_term_weights.push_back(static_cast<float>(count));
        }
    } else {
        throw std::domain_error("Query not parsed.");
    }
}

auto QueryRequest::term_ids() const -> gsl::span<std::uint32_t const>
{
    return gsl::span<std::uint32_t const>(m_term_ids);
}

auto QueryRequest::term_weights() const -> gsl::span<float const>
{
    return gsl::span<float const>(m_term_weights);
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
    std::vector<std::pair<std::size_t, float>> thresholds;

    [[nodiscard]] auto operator==(QueryContainerInner const& other) const noexcept -> bool
    {
        return id == other.id && query_string == other.query_string
            && processed_terms == other.processed_terms && term_ids == other.term_ids
            && thresholds == other.thresholds;
    }
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

auto QueryContainer::operator==(QueryContainer const& other) const noexcept -> bool
{
    return *m_data == *other.m_data;
}

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

auto QueryContainer::threshold(std::size_t k) const noexcept -> std::optional<float>
{
    auto pos = std::find_if(m_data->thresholds.begin(), m_data->thresholds.end(), first_equal_to(k));
    if (pos == m_data->thresholds.end()) {
        return std::nullopt;
    }
    return std::make_optional(pos->second);
}

auto QueryContainer::thresholds() const noexcept -> std::vector<std::pair<std::size_t, float>> const&
{
    return m_data->thresholds;
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
    m_data->processed_terms = std::move(processed_terms);
    return *this;
}

auto QueryContainer::add_threshold(std::size_t k, float score) -> bool
{
    if (auto pos =
            std::find_if(m_data->thresholds.begin(), m_data->thresholds.end(), first_equal_to(k));
        pos != m_data->thresholds.end()) {
        pos->second = score;
        return true;
    }
    m_data->thresholds.emplace_back(k, score);
    return false;
}

auto QueryContainer::query(std::size_t k) const -> QueryRequest
{
    return QueryRequest(*this, k);
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
        if (auto thresholds = json.find("thresholds"); thresholds != json.end()) {
            auto raise_error = [&]() {
                throw std::runtime_error(
                    fmt::format("Field \"thresholds\" is invalid: {}", thresholds->dump()));
            };
            if (not thresholds->is_array()) {
                raise_error();
            }
            for (auto&& threshold_entry: *thresholds) {
                if (not threshold_entry.is_object()) {
                    raise_error();
                }
                auto k = get<std::size_t>(threshold_entry, "k");
                auto score = get<float>(threshold_entry, "score");
                if (not k or not score) {
                    raise_error();
                }
                data.thresholds.emplace_back(*k, *score);
            }
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
auto QueryContainer::to_json_string() const -> std::string
{
    return to_json().dump();
}

auto QueryContainer::to_json() const -> nlohmann::json
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
    if (not m_data->thresholds.empty()) {
        auto thresholds = nlohmann::json::array();
        for (auto&& [k, score]: m_data->thresholds) {
            auto entry = nlohmann::json::object();
            entry["k"] = k;
            entry["score"] = score;
            thresholds.push_back(std::move(entry));
        }
        json["thresholds"] = thresholds;
    }
    return json;
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

void QueryContainer::filter_terms(gsl::span<std::size_t const> term_positions)
{
    auto const& processed_terms = m_data->processed_terms;
    auto const& term_ids = m_data->term_ids;
    if (not processed_terms && not term_ids) {
        return;
    }
    auto query_length = 0;
    if (processed_terms) {
        query_length = processed_terms->size();
    } else if (term_ids) {
        query_length = term_ids->size();
    }
    std::vector<std::string> filtered_terms;
    std::vector<TermId> filtered_ids;
    for (auto position: term_positions) {
        if (position >= query_length) {
            throw std::out_of_range("Passed term position out of range");
        }
        if (processed_terms) {
            filtered_terms.push_back(std::move((*m_data->processed_terms)[position]));
        }
        if (term_ids) {
            filtered_ids.push_back((*m_data->term_ids)[position]);
        }
    }
    if (processed_terms) {
        m_data->processed_terms = filtered_terms;
    }
    if (term_ids) {
        m_data->term_ids = filtered_ids;
    }
}

auto QueryReader::from_file(std::string const& file) -> QueryReader
{
    auto input = std::make_unique<std::ifstream>(file);
    auto& ref = *input;
    return QueryReader(std::move(input), ref);
}

auto QueryReader::from_stdin() -> QueryReader
{
    return QueryReader(nullptr, std::cin);
}

QueryReader::QueryReader(std::unique_ptr<std::istream> input, std::istream& stream_ref)
    : m_stream(std::move(input)), m_stream_ref(stream_ref)
{}

auto QueryReader::next() -> std::optional<QueryContainer>
{
    if (std::getline(m_stream_ref, m_line_buf)) {
        if (m_format) {
            if (*m_format == Format::Json) {
                return QueryContainer::from_json(m_line_buf);
            }
            return QueryContainer::from_colon_format(m_line_buf);
        }
        try {
            auto query = QueryContainer::from_json(m_line_buf);
            m_format = Format::Json;
            return query;
        } catch (std::exception const& err) {
            m_format = Format::Colon;
            return QueryContainer::from_colon_format(m_line_buf);
        }
    }
    return std::nullopt;
}

}  // namespace pisa
