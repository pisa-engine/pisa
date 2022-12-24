#include "token_stream.hpp"

namespace pisa {

TokenIterator::TokenIterator(TokenStream* tokenizer) : m_tokenizer(tokenizer), m_pos(0)
{
    m_token = m_tokenizer == nullptr ? std::nullopt : m_tokenizer->next();
}

[[nodiscard]] auto TokenIterator::operator*() -> value_type
{
    return *m_token;
}

auto TokenIterator::operator++() -> TokenIterator&
{
    if (m_token.has_value()) {
        m_token = m_tokenizer->next();
        ++m_pos;
    }
    return *this;
}

[[nodiscard]] auto TokenIterator::operator++(int) -> TokenIterator
{
    auto copy = *this;
    ++(*this);
    return copy;
}

[[nodiscard]] auto TokenIterator::operator==(TokenIterator const& other) -> bool
{
    if (m_token.has_value() && other.m_token.has_value()) {
        return m_pos == other.m_pos;
    }
    return m_token.has_value() == other.m_token.has_value();
}

[[nodiscard]] auto TokenIterator::operator!=(TokenIterator const& other) -> bool
{
    return !(*this == other);
}

TokenStream::TokenStream() = default;
TokenStream::TokenStream(TokenStream const&) = default;
TokenStream::TokenStream(TokenStream&&) = default;
TokenStream& TokenStream::operator=(TokenStream const&) = default;
TokenStream& TokenStream::operator=(TokenStream&&) = default;
TokenStream::~TokenStream() = default;

auto TokenStream::begin() -> TokenIterator
{
    return TokenIterator(this);
}

auto TokenStream::end() -> TokenIterator
{
    return TokenIterator(nullptr);
}

auto TokenStream::collect() -> std::vector<std::string>
{
    return std::vector<std::string>(begin(), end());
}

auto EmptyTokenStream::next() -> std::optional<std::string>
{
    return std::nullopt;
}

SingleTokenStream::SingleTokenStream(std::string token) : m_next(std::move(token)) {}

auto SingleTokenStream::next() -> std::optional<std::string>
{
    if (!m_next) {
        return std::nullopt;
    }
    auto ret = std::move(*m_next);
    m_next = std::nullopt;
    return ret;
}

}  // namespace pisa
