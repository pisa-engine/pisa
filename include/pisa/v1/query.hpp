#pragma once

#include <bitset>
#include <cstdint>
//#include <functional>
#include <vector>

#include <nlohmann/json_fwd.hpp>
#include <range/v3/action/unique.hpp>
#include <range/v3/algorithm/sort.hpp>
#include <tl/optional.hpp>

#include "topk_queue.hpp"
#include "v1/cursor/for_each.hpp"
#include "v1/cursor_intersection.hpp"
#include "v1/cursor_union.hpp"
#include "v1/inspect_query.hpp"
#include "v1/intersection.hpp"
#include "v1/types.hpp"

namespace pisa::v1 {

struct ListSelection {
    std::vector<TermId> unigrams{};
    std::vector<std::pair<TermId, TermId>> bigrams{};
};

struct TermIdSet {
    explicit TermIdSet(std::vector<TermId> terms) : m_term_list(std::move(terms))
    {
        m_term_set = m_term_list;
        ranges::sort(m_term_set);
        ranges::actions::unique(m_term_set);
        std::size_t pos = 0;
        for (auto term_id : m_term_set) {
            m_sorted_positions[term_id] = pos++;
        }
    }

    [[nodiscard]] auto sorted_position(TermId term) const -> std::size_t
    {
        return m_sorted_positions.at(term);
    }

    [[nodiscard]] auto term_at_pos(std::size_t pos) const -> TermId
    {
        if (pos >= m_term_list.size()) {
            throw std::out_of_range("Invalid intersections: term position out of bounds");
        }
        return m_term_list[pos];
    }

    [[nodiscard]] auto get() const -> std::vector<TermId> const& { return m_term_set; }

   private:
    friend std::ostream& operator<<(std::ostream& os, TermIdSet const& term_ids);
    std::vector<TermId> m_term_list;
    std::vector<TermId> m_term_set{};
    std::unordered_map<TermId, std::size_t> m_sorted_positions{};
};

struct Query {
    Query() = default;

    explicit Query(std::string query, tl::optional<std::string> id = tl::nullopt);
    explicit Query(std::vector<TermId> term_ids, tl::optional<std::string> id = tl::nullopt);

    /// Setters for optional values (or ones with default value).
    auto term_ids(std::vector<TermId> term_ids) -> Query&;
    auto id(std::string) -> Query&;
    auto k(int k) -> Query&;
    auto selections(gsl::span<std::bitset<64> const> selections) -> Query&;
    auto selections(ListSelection selections) -> Query&;
    auto threshold(float threshold) -> Query&;
    auto probability(float probability) -> Query&;

    /// Non-throwing getters
    [[nodiscard]] auto term_ids() const -> tl::optional<std::vector<TermId> const&>;
    [[nodiscard]] auto id() const -> tl::optional<std::string> const&;
    [[nodiscard]] auto k() const -> int;
    [[nodiscard]] auto selections() const -> tl::optional<ListSelection const&>;
    [[nodiscard]] auto threshold() const -> tl::optional<float>;
    [[nodiscard]] auto probability() const -> tl::optional<float>;
    [[nodiscard]] auto raw() const -> tl::optional<std::string const&>;

    /// Throwing getters
    [[nodiscard]] auto get_term_ids() const -> std::vector<TermId> const&;
    [[nodiscard]] auto get_id() const -> std::string const&;
    [[nodiscard]] auto get_selections() const -> ListSelection const&;
    [[nodiscard]] auto get_threshold() const -> float;
    [[nodiscard]] auto get_probability() const -> float;
    [[nodiscard]] auto get_raw() const -> std::string const&;

    [[nodiscard]] auto sorted_position(TermId term) const -> std::size_t;
    [[nodiscard]] auto term_at_pos(std::size_t pos) const -> TermId;

    template <typename Parser>
    [[nodiscard]] auto parse(Parser&& parser)
    {
        parser(*this);
    }

    void add_selections(gsl::span<std::bitset<64> const> selections);

    [[nodiscard]] auto filtered_terms(std::bitset<64> selection) const -> std::vector<TermId>;
    [[nodiscard]] auto to_json() const -> std::unique_ptr<nlohmann::json>;
    [[nodiscard]] static auto from_json(std::string_view) -> Query;
    [[nodiscard]] static auto from_plain(std::string_view) -> Query;

   private:
    friend std::ostream& operator<<(std::ostream& os, Query const& query);
    auto resolve_term(std::size_t pos) -> TermId;

    tl::optional<TermIdSet> m_term_ids{};
    tl::optional<ListSelection> m_selections{};
    tl::optional<float> m_threshold{};
    tl::optional<std::string> m_id{};
    tl::optional<std::string> m_raw_string;
    tl::optional<float> m_probability;
    int m_k = 1000;
};

template <typename T>
std::ostream& operator<<(std::ostream& os, tl::optional<T> const& value)
{
    if (not value) {
        os << "None";
    } else {
        os << "Some(" << *value << ")";
    }
    return os;
}

template <typename T1, typename T2>
std::ostream& operator<<(std::ostream& os, std::pair<T1, T2> const& p)
{
    return os << '(' << p.first << ", " << p.second << ')';
}

template <typename T>
std::ostream& operator<<(std::ostream& os, std::vector<T> const& vec)
{
    auto pos = vec.begin();
    os << '[';
    if (pos != vec.end()) {
        os << *pos++;
    }
    for (; pos != vec.end(); ++pos) {
        os << ' ' << *pos;
    }
    os << ']';
    return os;
}

inline std::ostream& operator<<(std::ostream& os, ListSelection const& selection)
{
    return os << "ListSelection { unigrams: " << selection.unigrams
              << ", bigrams: " << selection.bigrams << " }";
}

inline std::ostream& operator<<(std::ostream& os, TermIdSet const& term_ids)
{
    return os << "TermIdSet { original: " << term_ids.m_term_list
              << ", unique: " << term_ids.m_term_set << " }";
}

inline std::ostream& operator<<(std::ostream& os, Query const& query)
{
    return os << "Query { term_ids: " << query.m_term_ids << ", selections: " << query.m_selections
              << " }";
}

/// Returns only unique terms, in sorted order.
[[nodiscard]] auto filter_unique_terms(Query const& query) -> std::vector<TermId>;

} // namespace pisa::v1
