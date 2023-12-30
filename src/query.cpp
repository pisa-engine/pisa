// Copyright 2024 PISA Developers
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "query.hpp"

#include <unordered_map>
#include <unordered_set>

namespace pisa {

namespace query {

    constexpr auto TermPolicy::contains(TermPolicy const& other) const noexcept -> bool {
        return (this->policy & other.policy) > 0;
    }

    auto operator|(query::TermPolicy lhs, query::TermPolicy rhs) noexcept -> query::TermPolicy {
        return query::TermPolicy{lhs.policy | rhs.policy};
    }

}  // namespace query

auto Query::id() const noexcept -> std::optional<std::string_view> {
    if (m_id) {
        return std::string_view(*m_id);
    }
    return std::nullopt;
}

auto Query::terms() const noexcept -> std::vector<WeightedTerm> const& {
    return m_terms;
}

/** The first occurrence of each term is assigned the accumulated weight. */
void accumualte_weights(std::vector<WeightedTerm>& terms) {
    std::unordered_map<TermId, decltype(terms.begin())> positions;
    for (auto it = terms.begin(); it != terms.end(); ++it) {
        if (auto pos = positions.find(it->id); pos != positions.end()) {
            pos->second->weight += 1.0;
        } else {
            positions[it->id] = it;
        }
    }
}

void dedup_by_term_id(std::vector<WeightedTerm>& terms) {
    auto out = terms.begin();
    std::unordered_set<TermId> seen_terms;
    for (auto const& [term_id, weight]: terms) {
        if (seen_terms.find(term_id) == seen_terms.end()) {
            *out++ = {term_id, weight};
            seen_terms.insert(term_id);
        }
    }
    terms.erase(out, terms.end());
}

void Query::postprocess(query::TermPolicy policy) {
    if (!policy.contains(query::keep_duplicates)) {
        if (!policy.contains(query::unweighted)) {
            accumualte_weights(m_terms);
        }
        dedup_by_term_id(m_terms);
    }
    if (policy.contains(query::sort)) {
        std::sort(m_terms.begin(), m_terms.end(), [](auto const& lhs, auto const& rhs) {
            return lhs.id < rhs.id;
        });
    }
}

}  // namespace pisa
