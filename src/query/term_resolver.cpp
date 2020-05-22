#include "query/term_resolver.hpp"
#include "query/query_parser.hpp"
#include "query/term_processor.hpp"

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

    m_self->transform = pisa::term_processor_builder(stemmer_type)();

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

void filter_queries(
    std::optional<std::string> const& query_file,
    std::optional<TermResolver> term_resolver,
    std::size_t min_query_len,
    std::size_t max_query_len,
    std::ostream& out)
{
    auto reader = [&] {
        if (query_file) {
            return QueryReader::from_file(*query_file);
        }
        return QueryReader::from_stdin();
    }();
    reader.for_each([&](auto query) {
        if (not query.term_ids()) {
            if (not term_resolver) {
                throw MissingResolverError{};
            }
            query.parse(QueryParser(*term_resolver));
        }
        if (auto len = query.term_ids()->size(); len >= min_query_len && len <= max_query_len) {
            out << query.to_json() << '\n';
        }
    });
}

}  // namespace pisa
