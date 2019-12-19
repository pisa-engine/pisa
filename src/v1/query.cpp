#include <unordered_set>

#include <nlohmann/json.hpp>
#include <range/v3/algorithm.hpp>
#include <range/v3/view/concat.hpp>

#include "v1/query.hpp"

namespace pisa::v1 {

using json = nlohmann::json;

[[nodiscard]] auto filter_unique_terms(Query const& query) -> std::vector<TermId>
{
    auto terms = query.get_term_ids();
    std::sort(terms.begin(), terms.end());
    terms.erase(std::unique(terms.begin(), terms.end()), terms.end());
    return terms;
}

void Query::add_selections(gsl::span<std::bitset<64> const> selections)
{
    m_selections = ListSelection{};
    for (auto intersection : selections) {
        if (intersection.count() > 2) {
            throw std::invalid_argument("Intersections of more than 2 terms not supported yet!");
        }
        auto positions = to_vector(intersection);
        if (positions.size() == 1) {
            m_selections->unigrams.push_back(resolve_term(positions.front()));
        } else {
            m_selections->bigrams.emplace_back(resolve_term(positions[0]),
                                               resolve_term(positions[1]));
            auto& added = m_selections->bigrams.back();
            if (added.first > added.second) {
                std::swap(added.first, added.second);
            }
        }
    }
    ranges::sort(m_selections->unigrams);
    ranges::sort(m_selections->bigrams);
}

[[nodiscard]] auto Query::filtered_terms(std::bitset<64> selection) const -> std::vector<TermId>
{
    auto&& term_ids = get_term_ids();
    std::vector<TermId> terms;
    std::vector<float> weights;
    for (std::size_t bitpos = 0; bitpos < term_ids.size(); ++bitpos) {
        if (((1U << bitpos) & selection.to_ulong()) > 0) {
            terms.push_back(term_ids.at(bitpos));
        }
    }
    return terms;
}

auto Query::resolve_term(std::size_t pos) -> TermId
{
    if (not m_term_ids) {
        throw std::runtime_error("Term IDs are not set");
    }
    return m_term_ids->term_at_pos(pos);
}

template <typename T>
[[nodiscard]] auto get(json const& node, std::string_view field) -> tl::optional<T>
{
    if (auto pos = node.find(field); pos != node.end()) {
        return tl::make_optional(pos->get<T>());
    }
    return tl::optional<T>{};
}

[[nodiscard]] auto Query::from_plain(std::string_view query_string) -> Query
{
    auto colon = std::find(query_string.begin(), query_string.end(), ':');
    tl::optional<std::string> id;
    if (colon != query_string.end()) {
        id = std::string(query_string.begin(), colon);
    }
    auto pos = colon == query_string.end() ? query_string.begin() : std::next(colon);
    return Query(std::string(&*pos, std::distance(pos, query_string.end())), std::move(id));
}

[[nodiscard]] auto Query::from_json(std::string_view json_string) -> Query
{
    try {
        auto query_json = json::parse(json_string);
        auto id = get<std::string>(query_json, "id");
        auto raw_string = get<std::string>(query_json, "query");
        auto term_ids = get<std::vector<TermId>>(query_json, "term_ids");
        Query query = [&]() {
            if (raw_string) {
                auto query = Query(*raw_string.take(), id);
                if (term_ids) {
                    query.term_ids(*term_ids.take());
                }
                return query;
            }
            if (term_ids) {
                return Query(*term_ids.take(), id);
            }
            throw std::invalid_argument(
                "Failed to parse query: must define either raw string or term IDs");
        }();
        if (auto threshold = get<float>(query_json, "threshold"); threshold) {
            query.threshold(*threshold);
        }
        if (auto k = get<int>(query_json, "k"); k) {
            query.k(*k);
        }
        if (auto selections = get<std::vector<std::size_t>>(query_json, "selections"); selections) {
            std::vector<std::bitset<64>> bitsets;
            std::transform(selections->begin(),
                           selections->end(),
                           std::back_inserter(bitsets),
                           [](auto selection) { return std::bitset<64>(selection); });
            query.selections(gsl::span<std::bitset<64>>(bitsets));
        }
        return query;
    } catch (json::parse_error const& err) {
        throw std::runtime_error(fmt::format("Failed to parse query: {}", err.what()));
    }
}

[[nodiscard]] auto Query::to_json() const -> nlohmann::json
{
    json query;
    if (m_id) {
        query["id"] = *m_id;
    }
    if (m_raw_string) {
        query["query"] = *m_raw_string;
    }
    if (m_term_ids) {
        query["term_ids"] = m_term_ids->get();
    }
    if (m_threshold) {
        query["threshold"] = *m_threshold;
    }
    // TODO(michal)
    // tl::optional<ListSelection> m_selections{};
    // int m_k = 1000;
    return query;
}

Query::Query(std::vector<TermId> term_ids, tl::optional<std::string> id)
    : m_term_ids(std::move(term_ids)), m_id(std::move(id))
{
}

Query::Query(std::string query, tl::optional<std::string> id)
    : m_raw_string(std::move(query)), m_id(std::move(id))
{
}

auto Query::term_ids(std::vector<TermId> term_ids) -> Query&
{
    m_term_ids = TermIdSet(std::move(term_ids));
    return *this;
}

auto Query::id(std::string id) -> Query&
{
    m_id = std::move(id);
    return *this;
}

auto Query::k(int k) -> Query&
{
    m_k = k;
    return *this;
}

auto Query::selections(gsl::span<std::bitset<64> const> selections) -> Query&
{
    add_selections(selections);
    return *this;
}

auto Query::selections(ListSelection selections) -> Query&
{
    m_selections = std::move(selections);
    return *this;
}

auto Query::threshold(float threshold) -> Query&
{
    m_threshold = threshold;
    return *this;
}

auto Query::term_ids() const -> tl::optional<std::vector<TermId> const&>
{
    return m_term_ids.map(
        [](auto const& terms) -> std::vector<TermId> const& { return terms.get(); });
}
auto Query::id() const -> tl::optional<std::string> const& { return m_id; }
auto Query::k() const -> int { return m_k; }
auto Query::selections() const -> tl::optional<ListSelection const&>
{
    if (m_selections) {
        return *m_selections;
    }
    return tl::nullopt;
}
auto Query::threshold() const -> tl::optional<float> { return m_threshold; }
auto Query::raw() const -> tl::optional<std::string const&>
{
    if (m_raw_string) {
        return *m_raw_string;
    }
    return tl::nullopt;
}

/// Throwing getters
auto Query::get_term_ids() const -> std::vector<TermId> const&
{
    if (not m_term_ids) {
        throw std::runtime_error("Term IDs are not set");
    }
    return m_term_ids->get();
}

auto Query::get_id() const -> std::string const&
{
    if (not m_id) {
        throw std::runtime_error("ID is not set");
    }
    return *m_id;
}

auto Query::get_selections() const -> ListSelection const&
{
    if (not m_selections) {
        throw std::runtime_error("Selections are not set");
    }
    return *m_selections;
}

auto Query::get_threshold() const -> float
{
    if (not m_threshold) {
        throw std::runtime_error("Threshold is not set");
    }
    return *m_threshold;
}

auto Query::get_raw() const -> std::string const&
{
    if (not m_raw_string) {
        throw std::runtime_error("Raw query string is not set");
    }
    return *m_raw_string;
}

auto Query::sorted_position(TermId term) const -> std::size_t
{
    if (not m_term_ids) {
        throw std::runtime_error("Term IDs are not set");
    }
    return m_term_ids->sorted_position(term);
}

auto Query::term_at_pos(std::size_t pos) const -> TermId
{
    if (not m_term_ids) {
        throw std::runtime_error("Term IDs are not set");
    }
    return m_term_ids->term_at_pos(pos);
}

} // namespace pisa::v1
