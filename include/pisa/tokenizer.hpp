#pragma once

#include <cctype>
#include <optional>
#include <string>
#include <string_view>

#include <boost/spirit/include/lex_lexertl.hpp>
#include <boost/spirit/include/qi.hpp>

namespace pisa {

class TokenStream;

/**
 * C++ style iterator wrapping a tokenizer.
 */
class TokenIterator {
    TokenStream* m_tokenizer;
    std::size_t m_pos;
    std::optional<std::string> m_token;

  public:
    using iterator_category = std::input_iterator_tag;
    using value_type = std::string;
    using difference_type = std::ptrdiff_t;
    using pointer = value_type*;
    using reference = value_type const&;

    explicit TokenIterator(TokenStream* tokenizer);

    [[nodiscard]] auto operator*() -> value_type;
    auto operator++() -> TokenIterator&;
    [[nodiscard]] auto operator++(int) -> TokenIterator;
    [[nodiscard]] auto operator==(TokenIterator const& other) -> bool;
    [[nodiscard]] auto operator!=(TokenIterator const& other) -> bool;
};

/**
 * Token stream abstraction. Typically takes an input string and produces consecutive string tokens.
 */
class TokenStream {
  public:
    /**
     * Returns the next token or `std::nullopt` if no more tokens are available.
     */
    virtual auto next() -> std::optional<std::string> = 0;

    /**
     * Returns the iterator pointing to the beginning of the token stream.
     *
     * Note that this method should be called only once, as any iterator consumes processed tokens.
     */
    [[nodiscard]] virtual auto begin() -> TokenIterator;

    /**
     * Returns the iterator pointing to the end of the token stream.
     */
    [[nodiscard]] virtual auto end() -> TokenIterator;
};

class WhitespaceTokenStream: public TokenStream {
    std::string_view m_input;

  public:
    explicit WhitespaceTokenStream(std::string_view input);
    virtual auto next() -> std::optional<std::string> override;
};

namespace lex = boost::spirit::lex;

using token_type =
    lex::lexertl::token<std::string_view::const_iterator, boost::mpl::vector<>, boost::mpl::false_>;
using lexer_type = lex::lexertl::actor_lexer<token_type>;

struct Lexer: lex::lexer<lexer_type> {
    Lexer();
};

class EnglishTokenStream: public TokenStream {
    static Lexer const LEXER;

    using iterator = typename lexer_type::iterator_type;

    typename std::string_view::const_iterator m_begin;
    typename std::string_view::const_iterator m_end;
    iterator m_pos;
    iterator m_sentinel;

  public:
    explicit EnglishTokenStream(std::string_view input);
    virtual auto next() -> std::optional<std::string> override;
};

/**
 * Tokenizer abstraction.
 */
class Tokenizer {
  public:
    /**
     * Constructs a token stream corresponding to the given string input.
     */
    [[nodiscard]] virtual auto tokenize(std::string_view input) const
        -> std::unique_ptr<TokenStream> = 0;
};

/**
 * Splits words on any number of consecutive whitespaces.
 */
class WhitespaceTokenizer: public Tokenizer {
  public:
    [[nodiscard]] virtual auto tokenize(std::string_view input) const
        -> std::unique_ptr<TokenStream> override;
};

/**
 * English token stream.
 *
 * There are three types of valid tokens:
 *  - abbreviations, such as "U.S.A.", for which periods are removed,
 *  - possessives, such as "dog's", for which everything up to the apostrophe is returned,
 *  - any other alphanumeric sequences.
 */
class EnglishTokenizer: public Tokenizer {
  public:
    [[nodiscard]] virtual auto tokenize(std::string_view input) const
        -> std::unique_ptr<TokenStream> override;
};

}  // namespace pisa
