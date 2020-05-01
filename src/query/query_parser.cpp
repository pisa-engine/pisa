#include <mio/mmap.hpp>

#include "io.hpp"
#include "payload_vector.hpp"
#include "query.hpp"
#include "query/query_parser.hpp"
#include "query/term_resolver.hpp"
#include "tokenizer.hpp"

namespace pisa {

QueryParser::QueryParser(TermResolver term_resolver) : m_term_resolver(std::move(term_resolver)) {}

auto QueryParser::operator()(std::string const& query) -> std::vector<ResolvedTerm>
{
    TermTokenizer tokenizer(query);
    std::vector<ResolvedTerm> terms;
    for (auto term_iter = tokenizer.begin(); term_iter != tokenizer.end(); ++term_iter) {
        auto term = m_term_resolver(*term_iter);
        if (term) {
            terms.push_back(std::move(*term));
        }
    }
    return terms;
}

}  // namespace pisa
