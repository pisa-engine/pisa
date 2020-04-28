#include <mio/mmap.hpp>

#include "io.hpp"
#include "payload_vector.hpp"
#include "query.hpp"
#include "query/parser.hpp"
#include "query/term_processor.hpp"
#include "tokenizer.hpp"

namespace pisa {

StandardTermResolver::StandardTermResolver(StandardTermResolver const& other)
    : m_self(std::make_unique<StandardTermResolverParams>(*other.m_self))
{}
StandardTermResolver::StandardTermResolver(StandardTermResolver&&) noexcept = default;
StandardTermResolver& StandardTermResolver::operator=(StandardTermResolver const& other)
{
    m_self = std::make_unique<StandardTermResolverParams>(*other.m_self);
    return *this;
}
StandardTermResolver& StandardTermResolver::operator=(StandardTermResolver&&) noexcept = default;
StandardTermResolver::~StandardTermResolver() = default;

struct StandardTermResolverParams {
    std::vector<std::uint32_t> stopwords;
    std::function<std::optional<std::uint32_t>(std::string const&)> to_id;
    std::function<std::string(std::string)> transform;
};

StandardTermResolver::StandardTermResolver(
    std::string const& term_lexicon_path,
    std::optional<std::string> const& stopwords_filename,
    std::optional<std::string> const& stemmer_type)
    : m_self(std::make_unique<StandardTermResolverParams>())
{
    auto source = std::make_shared<mio::mmap_source>(term_lexicon_path.c_str());
    auto terms = pisa::Payload_Vector<>::from(*source);

    m_self->to_id = [source = std::move(source), terms](auto str) -> std::optional<std::uint32_t> {
        auto pos = std::lower_bound(terms.begin(), terms.end(), std::string_view(str));
        if (*pos == std::string_view(str)) {
            return std::distance(terms.begin(), pos);
        }
        return std::nullopt;
    };

    m_self->transform = pisa::term_processor(stemmer_type);

    if (stopwords_filename) {
        std::ifstream is(*stopwords_filename);
        pisa::io::for_each_line(is, [&](auto&& word) {
            if (auto term_id = m_self->to_id(std::move(word)); term_id.has_value()) {
                m_self->stopwords.push_back(*term_id);
            }
        });
        std::sort(m_self->stopwords.begin(), m_self->stopwords.end());
    }
}

auto StandardTermResolver::operator()(std::string token) const -> std::optional<ResolvedTerm>
{
    token = m_self->transform(token);
    auto id = m_self->to_id(token);
    if (not id) {
        return std::nullopt;
    }
    if (is_stopword(*id)) {
        return std::nullopt;
    }
    return pisa::ResolvedTerm{*id, token};
}

auto StandardTermResolver::is_stopword(std::uint32_t const term) const -> bool
{
    auto pos = std::lower_bound(m_self->stopwords.begin(), m_self->stopwords.end(), term);
    return pos != m_self->stopwords.end() && *pos == term;
}

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
