#include <range/v3/algorithm.hpp>

#include "v1/query.hpp"

namespace pisa::v1 {

[[nodiscard]] auto filter_unique_terms(Query const& query) -> std::vector<TermId>
{
    auto terms = query.terms;
    std::sort(terms.begin(), terms.end());
    terms.erase(std::unique(terms.begin(), terms.end()), terms.end());
    return terms;
}

void Query::add_selections(gsl::span<std::bitset<64> const> selections)
{
    list_selection = ListSelection{};
    for (auto intersection : selections) {
        if (intersection.count() > 2) {
            throw std::invalid_argument("Intersections of more than 2 terms not supported yet!");
        }
        auto positions = to_vector(intersection);
        if (positions.size() == 1) {
            list_selection->unigrams.push_back(resolve_term(positions.front()));
        } else {
            list_selection->bigrams.emplace_back(resolve_term(positions[0]),
                                                 resolve_term(positions[1]));
        }
    }
}

void Query::remove_duplicates()
{
    ranges::sort(terms);
    ranges::actions::unique(terms);
    if (list_selection) {
        ranges::sort(list_selection->unigrams);
        ranges::actions::unique(list_selection->unigrams);
        ranges::sort(list_selection->bigrams);
        ranges::actions::unique(list_selection->bigrams);
    }
}

auto Query::resolve_term(std::size_t pos) -> TermId
{
    if (pos >= terms.size()) {
        throw std::out_of_range("Invalid intersections: term position out of bounds");
    }
    return terms[pos];
}

} // namespace pisa::v1
