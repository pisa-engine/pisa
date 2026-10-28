#include "tokenizer.hpp"

#include <cctype>
#include <stdexcept>
#include <string>

#if defined(PISA_HAS_ICU)
#include <unicode/brkiter.h>
#include <unicode/locid.h>
#include <unicode/ubrk.h>
#include <unicode/unistr.h>
#include <unicode/utypes.h>
#endif

namespace pisa {

enum TokenType { Abbreviature = 1, Possessive = 2, Term = 3, NotValid = 4 };

[[nodiscard]] auto is_space(char symbol) -> bool {
    return std::isspace(static_cast<unsigned char>(symbol)) != 0;
}

auto is_valid(token_type const& tok) -> bool {
    return tok.id() != TokenType::NotValid;
}

WhitespaceTokenStream::WhitespaceTokenStream(WhitespaceTokenStream const&) = default;
WhitespaceTokenStream::WhitespaceTokenStream(WhitespaceTokenStream&&) = default;
WhitespaceTokenStream& WhitespaceTokenStream::operator=(WhitespaceTokenStream const&) = default;
WhitespaceTokenStream& WhitespaceTokenStream::operator=(WhitespaceTokenStream&&) = default;
WhitespaceTokenStream::~WhitespaceTokenStream() = default;

WhitespaceTokenStream::WhitespaceTokenStream(std::string_view input)
    : m_input(input), m_view(m_input.as_view()) {}
WhitespaceTokenStream::WhitespaceTokenStream(std::string input)
    : m_input(std::move(input)), m_view(m_input.as_view()) {}
WhitespaceTokenStream::WhitespaceTokenStream(CowString input)
    : m_input(std::move(input)), m_view(m_input.as_view()) {}

auto WhitespaceTokenStream::next() -> std::optional<std::string> {
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

auto transform_token(token_type const& tok) -> std::string {
    auto& val = tok.value();
    switch (tok.id()) {
    case TokenType::Abbreviature: {
        std::string term;
        std::copy_if(val.begin(), val.end(), std::back_inserter(term), [](char ch) {
            return ch != '.';
        });
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

Lexer::Lexer() {
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
      m_sentinel(LEXER.end()) {}

EnglishTokenStream::EnglishTokenStream(std::string input)
    : m_input(std::move(input)),
      m_begin(m_input.as_view().begin()),
      m_end(m_input.as_view().end()),
      m_pos(LEXER.begin(m_begin, m_end)),
      m_sentinel(LEXER.end()) {}

EnglishTokenStream::EnglishTokenStream(CowString input)
    : m_input(std::move(input)),
      m_begin(m_input.as_view().begin()),
      m_end(m_input.as_view().end()),
      m_pos(LEXER.begin(m_begin, m_end)),
      m_sentinel(LEXER.end()) {}

EnglishTokenStream::EnglishTokenStream(EnglishTokenStream const&) = default;
EnglishTokenStream::EnglishTokenStream(EnglishTokenStream&&) = default;
EnglishTokenStream& EnglishTokenStream::operator=(EnglishTokenStream const&) = default;
EnglishTokenStream& EnglishTokenStream::operator=(EnglishTokenStream&&) = default;
EnglishTokenStream::~EnglishTokenStream() = default;

auto EnglishTokenStream::next() -> std::optional<std::string> {
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

auto WhitespaceTokenizer::tokenize(std::string_view input) const -> std::unique_ptr<TokenStream> {
    return std::make_unique<WhitespaceTokenStream>(input);
}

auto WhitespaceTokenizer::tokenize(std::string input) const -> std::unique_ptr<TokenStream> {
    return std::make_unique<WhitespaceTokenStream>(std::move(input));
}

auto WhitespaceTokenizer::tokenize(CowString input) const -> std::unique_ptr<TokenStream> {
    return std::make_unique<WhitespaceTokenStream>(std::move(input));
}

auto EnglishTokenizer::tokenize(std::string_view input) const -> std::unique_ptr<TokenStream> {
    return std::make_unique<EnglishTokenStream>(input);
}

auto EnglishTokenizer::tokenize(std::string input) const -> std::unique_ptr<TokenStream> {
    return std::make_unique<EnglishTokenStream>(std::move(input));
}

auto EnglishTokenizer::tokenize(CowString input) const -> std::unique_ptr<TokenStream> {
    return std::make_unique<EnglishTokenStream>(std::move(input));
}

EnglishTokenizer::EnglishTokenizer() = default;
EnglishTokenizer::EnglishTokenizer(EnglishTokenizer const&) = default;
EnglishTokenizer::EnglishTokenizer(EnglishTokenizer&&) = default;
EnglishTokenizer& EnglishTokenizer::operator=(EnglishTokenizer const&) = default;
EnglishTokenizer& EnglishTokenizer::operator=(EnglishTokenizer&&) = default;
EnglishTokenizer::~EnglishTokenizer() = default;

#if defined(PISA_HAS_ICU)
// putting this in the header would require including ICU headers in every
// translation unit that includes tokenizer.hpp, which is not desirable
struct InternationalTokenStream::Impl {
    icu::UnicodeString text;
    std::unique_ptr<icu::BreakIterator> iterator;
    int32_t start{0};

    explicit Impl(std::string_view input) {
        // copy input to a UnicodeString and fold case for case-insensitive tokenization
        text = icu::UnicodeString::fromUTF8(icu::StringPiece(input.data(), input.size()));
        text.foldCase();

        UErrorCode status = U_ZERO_ERROR;
        iterator.reset(icu::BreakIterator::createWordInstance(icu::Locale::getRoot(), status));
        if (U_FAILURE(status) || iterator == nullptr) {
            throw std::runtime_error(
                std::string("Unable to initialize ICU word break iterator: ") + u_errorName(status)
            );
        }
        iterator->setText(text);
        start = iterator->first();
    }

    auto next() -> std::optional<std::string> {
        for (int32_t end = iterator->next(); end != icu::BreakIterator::DONE;
             end = iterator->next()) {
            auto const status = iterator->getRuleStatus();
            auto const segment_start = start;
            start = end;

            // Skip segments that are not words (whitespace, punctuation, symbols).
            if (status >= UBRK_WORD_NONE && status < UBRK_WORD_NONE_LIMIT) {
                continue;
            }

            std::string token;
            text.tempSubStringBetween(segment_start, end).toUTF8String(token);
            if (!token.empty()) {
                return token;
            }
        }
        return std::nullopt;
    }
};

InternationalTokenStream::InternationalTokenStream(std::string_view input)
    : m_impl(std::make_unique<Impl>(input)) {}

#else

// Without ICU there is no state to hold. Impl exists only so that the destructor and move
// operations below compile; no instance is ever created because the constructors throw.
struct InternationalTokenStream::Impl {
    auto next() -> std::optional<std::string> { return std::nullopt; }
};

InternationalTokenStream::InternationalTokenStream(std::string_view) {
    throw std::runtime_error(
        "InternationalTokenizer requires ICU support at build time. "
        "Install the ICU development files (libicu-dev on Debian/Ubuntu, "
        "libicu-devel on Fedora) and reconfigure with -DPISA_ENABLE_ICU=ON"
    );
}

#endif

InternationalTokenStream::InternationalTokenStream(std::string input)
    : InternationalTokenStream(std::string_view(input)) {}
InternationalTokenStream::InternationalTokenStream(CowString input)
    : InternationalTokenStream(input.as_view()) {}
InternationalTokenStream::InternationalTokenStream(InternationalTokenStream&&) noexcept = default;
InternationalTokenStream& InternationalTokenStream::operator=(InternationalTokenStream&&) noexcept =
    default;
InternationalTokenStream::~InternationalTokenStream() = default;

auto InternationalTokenStream::next() -> std::optional<std::string> {
    return m_impl->next();
}

auto InternationalTokenizer::tokenize(std::string_view input) const -> std::unique_ptr<TokenStream> {
    return std::make_unique<InternationalTokenStream>(input);
}

auto InternationalTokenizer::tokenize(std::string input) const -> std::unique_ptr<TokenStream> {
    return std::make_unique<InternationalTokenStream>(std::move(input));
}

auto InternationalTokenizer::tokenize(CowString input) const -> std::unique_ptr<TokenStream> {
    return std::make_unique<InternationalTokenStream>(std::move(input));
}

InternationalTokenizer::InternationalTokenizer() = default;
InternationalTokenizer::InternationalTokenizer(InternationalTokenizer const&) = default;
InternationalTokenizer::InternationalTokenizer(InternationalTokenizer&&) = default;
InternationalTokenizer& InternationalTokenizer::operator=(InternationalTokenizer const&) = default;
InternationalTokenizer& InternationalTokenizer::operator=(InternationalTokenizer&&) = default;
InternationalTokenizer::~InternationalTokenizer() = default;

}  // namespace pisa
