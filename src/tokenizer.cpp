#include "tokenizer.hpp"

#include <cctype>
#include <string>

namespace pisa {

enum TokenType { Abbreviature = 1, Possessive = 2, Term = 3, NotValid = 4 };

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

auto TokenStream::begin() -> TokenIterator
{
    return TokenIterator(this);
}

auto TokenStream::end() -> TokenIterator
{
    return TokenIterator(nullptr);
}

[[nodiscard]] auto is_space(char symbol) -> bool
{
    return std::isspace(static_cast<unsigned char>(symbol)) != 0;
}

auto is_valid(token_type const& tok) -> bool
{
    return tok.id() != TokenType::NotValid;
}

WhitespaceTokenStream::WhitespaceTokenStream(std::string_view input) : m_input(input) {}

auto WhitespaceTokenStream::next() -> std::optional<std::string>
{
    auto pos = std::find_if_not(m_input.begin(), m_input.end(), is_space);
    m_input = m_input.substr(std::distance(m_input.begin(), pos));
    if (m_input.empty()) {
        return std::nullopt;
    }
    pos = std::find_if(m_input.begin(), m_input.end(), is_space);
    auto token = m_input.substr(0, std::distance(m_input.begin(), pos));
    m_input = m_input.substr(std::distance(m_input.begin(), pos));
    return std::string(token);
}

auto transform_token(token_type const& tok) -> std::string
{
    auto& val = tok.value();
    switch (tok.id()) {
    case TokenType::Abbreviature: {
        std::string term;
        std::copy_if(
            val.begin(), val.end(), std::back_inserter(term), [](char ch) { return ch != '.'; });
        return term;
    }
    case TokenType::Possessive:
        return std::string(val.begin(), std::find(val.begin(), val.end(), '\''));
    default: return std::string(val.begin(), val.end());
    }
}

Lexer::Lexer()
{
    // Note: parsing process takes the first match from left to right.
    this->self = lex::token_def<>("([a-zA-Z]+\\.){2,}", TokenType::Abbreviature)
        | lex::token_def<>("[a-zA-Z0-9]+('[a-zA-Z]+)", TokenType::Possessive)
        | lex::token_def<>("[a-zA-Z0-9]+", TokenType::Term)
        | lex::token_def<>(".", TokenType::NotValid);
}

Lexer const EnglishTokenStream::LEXER = Lexer{};

EnglishTokenStream::EnglishTokenStream(std::string_view input)
    : m_begin(input.begin()),
      m_end(input.end()),
      m_pos(LEXER.begin(m_begin, m_end)),
      m_sentinel(LEXER.end())
{}

auto EnglishTokenStream::next() -> std::optional<std::string>
{
    while (m_pos != m_sentinel && !is_valid(*m_pos)) {
        ++m_pos;
    }
    if (m_pos == m_sentinel) {
        return std::nullopt;
    }
    auto token = transform_token(*m_pos);
    ++m_pos;
    return token;
}

auto WhitespaceTokenizer::tokenize(std::string_view input) const -> std::unique_ptr<TokenStream>
{
    return std::make_unique<WhitespaceTokenStream>(input);
}

auto EnglishTokenizer::tokenize(std::string_view input) const -> std::unique_ptr<TokenStream>
{
    return std::make_unique<EnglishTokenStream>(input);
}

}  // namespace pisa
