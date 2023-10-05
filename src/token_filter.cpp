#include "pisa/token_filter.hpp"

#include <cctype>

#include <boost/algorithm/string.hpp>

namespace pisa {

TokenFilter::TokenFilter() = default;
TokenFilter::TokenFilter(TokenFilter const&) = default;
TokenFilter::TokenFilter(TokenFilter&&) = default;
TokenFilter& TokenFilter::operator=(TokenFilter const&) = default;
TokenFilter& TokenFilter::operator=(TokenFilter&&) = default;
TokenFilter::~TokenFilter() = default;

auto Porter2Stemmer::filter(std::string_view input) const -> std::unique_ptr<TokenStream>
{
    return filter(std::string(input));
}

auto Porter2Stemmer::filter(std::string input) const -> std::unique_ptr<TokenStream>
{
    thread_local porter2::Stemmer stemmer{};
    return std::make_unique<SingleTokenStream>(stemmer.stem(input));
}

auto Porter2Stemmer::filter(CowString input) const -> std::unique_ptr<TokenStream>
{
    return filter(std::move(input).to_owned());
}

auto KrovetzStemmer::filter(std::string_view input) const -> std::unique_ptr<TokenStream>
{
    return filter(std::string(input));
}

auto KrovetzStemmer::filter(std::string input) const -> std::unique_ptr<TokenStream>
{
    return std::make_unique<SingleTokenStream>(m_stemmer->kstem_stemmer(input));
}

auto KrovetzStemmer::filter(CowString input) const -> std::unique_ptr<TokenStream>
{
    return filter(std::move(input).to_owned());
}

auto LowercaseFilter::filter(std::string_view input) const -> std::unique_ptr<TokenStream>
{
    return filter(std::string(input));
}

auto LowercaseFilter::filter(std::string input) const -> std::unique_ptr<TokenStream>
{
    boost::algorithm::to_lower(input);
    return std::make_unique<SingleTokenStream>(std::move(input));
}

auto LowercaseFilter::filter(CowString input) const -> std::unique_ptr<TokenStream>
{
    return filter(std::move(input).to_owned());
}

StopWordRemover::StopWordRemover(std::unordered_set<std::string> stopwords)
    : m_stopwords(std::move(stopwords))
{}

auto StopWordRemover::filter(std::string_view input) const -> std::unique_ptr<TokenStream>
{
    return filter(std::string(input));
}

auto StopWordRemover::filter(std::string input) const -> std::unique_ptr<TokenStream>
{
    if (m_stopwords.find(input) != m_stopwords.end()) {
        return std::make_unique<EmptyTokenStream>();
    }
    return std::make_unique<SingleTokenStream>(std::move(input));
}

auto StopWordRemover::filter(CowString input) const -> std::unique_ptr<TokenStream>
{
    return filter(std::move(input).to_owned());
}

}  // namespace pisa
