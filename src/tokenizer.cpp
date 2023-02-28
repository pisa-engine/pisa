#include "tokenizer.hpp"

#include <cctype>
#include <string>

namespace pisa {

enum TokenType { Abbreviature = 1, Possessive = 2, Term = 3, NotValid = 4 };

[[nodiscard]] auto is_space(char symbol) -> bool
{
    return std::isspace(static_cast<unsigned char>(symbol)) != 0;
}

auto is_valid(token_type const& tok) -> bool
{
    return tok.id() != TokenType::NotValid;
}

WhitespaceTokenStream::WhitespaceTokenStream(WhitespaceTokenStream const&) = default;
WhitespaceTokenStream::WhitespaceTokenStream(WhitespaceTokenStream&&) = default;
WhitespaceTokenStream& WhitespaceTokenStream::operator=(WhitespaceTokenStream const&) = default;
WhitespaceTokenStream& WhitespaceTokenStream::operator=(WhitespaceTokenStream&&) = default;
WhitespaceTokenStream::~WhitespaceTokenStream() = default;

WhitespaceTokenStream::WhitespaceTokenStream(std::string_view input)
    : m_input(input), m_view(m_input.as_view())
{}
WhitespaceTokenStream::WhitespaceTokenStream(std::string input)
    : m_input(std::move(input)), m_view(m_input.as_view())
{}
WhitespaceTokenStream::WhitespaceTokenStream(CowString input)
    : m_input(std::move(input)), m_view(m_input.as_view())
{}

auto WhitespaceTokenStream::next() -> std::optional<std::string>
{
    auto pos = std::find_if_not(m_view.begin(), m_view.end(), is_space);
    m_view = m_view.substr(std::distance(m_view.begin(), pos));
    if (m_view.empty()) {
        return std::nullopt;
    }
    pos = std::find_if(m_view.begin(), m_view.end(), is_space);
    auto token = m_view.substr(0, std::distance(m_view.begin(), pos));
    m_view = m_view.substr(std::distance(m_view.begin(), pos));
    return std::string(token);
}

WhitespaceTokenizer::WhitespaceTokenizer() = default;
WhitespaceTokenizer::WhitespaceTokenizer(WhitespaceTokenizer const&) = default;
WhitespaceTokenizer::WhitespaceTokenizer(WhitespaceTokenizer&&) = default;
WhitespaceTokenizer& WhitespaceTokenizer::operator=(WhitespaceTokenizer const&) = default;
WhitespaceTokenizer& WhitespaceTokenizer::operator=(WhitespaceTokenizer&&) = default;
WhitespaceTokenizer::~WhitespaceTokenizer() = default;

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

struct Lexer: lex::lexer<lexer_type> {
    Lexer();
};

Lexer::Lexer()
{
    // Note: parsing process takes the first match from left to right.
    this->self = lex::token_def<>("([a-zA-Z]+\\.){2,}", TokenType::Abbreviature)
        | lex::token_def<>("[a-zA-Z0-9]+('[a-zA-Z]+)", TokenType::Possessive)
        | lex::token_def<>("[a-zA-Z0-9]+", TokenType::Term)
        | lex::token_def<>(".", TokenType::NotValid);
}

thread_local Lexer const LEXER = Lexer{};

EnglishTokenStream::EnglishTokenStream(std::string_view input)
    : m_input(input),
      m_begin(input.begin()),
      m_end(input.end()),
      m_pos(LEXER.begin(m_begin, m_end)),
      m_sentinel(LEXER.end())
{}

EnglishTokenStream::EnglishTokenStream(std::string input)
    : m_input(std::move(input)),
      m_begin(m_input.as_view().begin()),
      m_end(m_input.as_view().end()),
      m_pos(LEXER.begin(m_begin, m_end)),
      m_sentinel(LEXER.end())
{}

EnglishTokenStream::EnglishTokenStream(CowString input)
    : m_input(std::move(input)),
      m_begin(m_input.as_view().begin()),
      m_end(m_input.as_view().end()),
      m_pos(LEXER.begin(m_begin, m_end)),
      m_sentinel(LEXER.end())
{}

EnglishTokenStream::EnglishTokenStream(EnglishTokenStream const&) = default;
EnglishTokenStream::EnglishTokenStream(EnglishTokenStream&&) = default;
EnglishTokenStream& EnglishTokenStream::operator=(EnglishTokenStream const&) = default;
EnglishTokenStream& EnglishTokenStream::operator=(EnglishTokenStream&&) = default;
EnglishTokenStream::~EnglishTokenStream() = default;

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

Tokenizer::Tokenizer() = default;
Tokenizer::Tokenizer(Tokenizer const&) = default;
Tokenizer::Tokenizer(Tokenizer&&) = default;
Tokenizer& Tokenizer::operator=(Tokenizer const&) = default;
Tokenizer& Tokenizer::operator=(Tokenizer&&) = default;
Tokenizer::~Tokenizer() = default;

auto WhitespaceTokenizer::tokenize(std::string_view input) const -> std::unique_ptr<TokenStream>
{
    return std::make_unique<WhitespaceTokenStream>(input);
}

auto WhitespaceTokenizer::tokenize(std::string input) const -> std::unique_ptr<TokenStream>
{
    return std::make_unique<WhitespaceTokenStream>(std::move(input));
}

auto WhitespaceTokenizer::tokenize(CowString input) const -> std::unique_ptr<TokenStream>
{
    return std::make_unique<WhitespaceTokenStream>(std::move(input));
}

auto EnglishTokenizer::tokenize(std::string_view input) const -> std::unique_ptr<TokenStream>
{
    return std::make_unique<EnglishTokenStream>(input);
}

auto EnglishTokenizer::tokenize(std::string input) const -> std::unique_ptr<TokenStream>
{
    return std::make_unique<EnglishTokenStream>(std::move(input));
}

auto EnglishTokenizer::tokenize(CowString input) const -> std::unique_ptr<TokenStream>
{
    return std::make_unique<EnglishTokenStream>(std::move(input));
}

EnglishTokenizer::EnglishTokenizer() = default;
EnglishTokenizer::EnglishTokenizer(EnglishTokenizer const&) = default;
EnglishTokenizer::EnglishTokenizer(EnglishTokenizer&&) = default;
EnglishTokenizer& EnglishTokenizer::operator=(EnglishTokenizer const&) = default;
EnglishTokenizer& EnglishTokenizer::operator=(EnglishTokenizer&&) = default;
EnglishTokenizer::~EnglishTokenizer() = default;

}  // namespace pisa
