#include "v1/query.hpp"

namespace pisa::v1 {

[[nodiscard]] auto filter_unique_terms(Query const& query) -> std::vector<TermId>
{
    auto terms = query.terms;
    std::sort(terms.begin(), terms.end());
    terms.erase(std::unique(terms.begin(), terms.end()), terms.end());
    return terms;
}

} // namespace pisa::v1
