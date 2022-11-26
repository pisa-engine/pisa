#pragma once

#include <cctype>
#include <optional>
#include <string>
#include <string_view>

#include <boost/spirit/include/lex_lexertl.hpp>
#include <boost/spirit/include/qi.hpp>

namespace pisa {

class Tokenizer;

/**
 * C++ style iterator wrapping a tokenizer.
 */
class TokenizerIterator {
    Tokenizer* m_tokenizer;
    std::size_t m_pos;
    std::optional<std::string> m_token;

  public:
    using iterator_category = std::input_iterator_tag;
    using value_type = std::string;
    using difference_type = std::ptrdiff_t;
    using pointer = value_type*;
    using reference = value_type const&;

    explicit TokenizerIterator(Tokenizer* tokenizer);

    [[nodiscard]] auto operator*() -> value_type;
    auto operator++() -> TokenizerIterator&;
    [[nodiscard]] auto operator++(int) -> TokenizerIterator;
    [[nodiscard]] auto operator==(TokenizerIterator const& other) -> bool;
    [[nodiscard]] auto operator!=(TokenizerIterator const& other) -> bool;
};

/**
 * Tokenizer abstraction. A tokenizer typically takes an input string and produces consecutive
 * string tokens.
 */
class Tokenizer {
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
    [[nodiscard]] virtual auto begin() -> TokenizerIterator;

    /**
     * Returns the iterator pointing to the end of the token stream.
     */
    [[nodiscard]] virtual auto end() -> TokenizerIterator;
};

/**
 * Whitespace tokenizer.
 *
 * This is a simple tokenizer that splits words on any number of consecutive whitespaces.
 */
class WhitespaceTokenizer: public Tokenizer {
    std::string_view m_input;

  public:
    explicit WhitespaceTokenizer(std::string_view input);
    virtual auto next() -> std::optional<std::string> override;
};

namespace lex = boost::spirit::lex;

using token_type =
    lex::lexertl::token<std::string_view::const_iterator, boost::mpl::vector<>, boost::mpl::false_>;
using lexer_type = lex::lexertl::actor_lexer<token_type>;

struct Lexer: lex::lexer<lexer_type> {
    Lexer();
};

/**
 * English tokenizer.
 *
 * There are three types of valid tokens:
 *  - abbreviations, such as "U.S.A.", for which periods are removed,
 *  - possessives, such as "dog's", for which everything up to the apostrophe is returned,
 *  - any other alphanumeric sequences.
 */
class EnglishTokenizer: public Tokenizer {
    static Lexer const LEXER;

    using iterator = typename lexer_type::iterator_type;

    typename std::string_view::const_iterator m_begin;
    typename std::string_view::const_iterator m_end;
    iterator m_pos;
    iterator m_sentinel;

  public:
    explicit EnglishTokenizer(std::string_view input);
    virtual auto next() -> std::optional<std::string> override;
};

}  // namespace pisa
