#pragma once

#include <cctype>
#include <optional>
#include <string>
#include <string_view>

#include <boost/spirit/include/lex_lexertl.hpp>
#include <boost/spirit/include/qi.hpp>

#include "cow_string.hpp"
#include "token_stream.hpp"

namespace pisa {

class WhitespaceTokenStream: public TokenStream {
    CowString m_input;
    std::string_view m_view;

  public:
    explicit WhitespaceTokenStream(std::string_view input);
    explicit WhitespaceTokenStream(std::string input);
    explicit WhitespaceTokenStream(CowString input);
    WhitespaceTokenStream(WhitespaceTokenStream const&);
    WhitespaceTokenStream(WhitespaceTokenStream&&);
    WhitespaceTokenStream& operator=(WhitespaceTokenStream const&);
    WhitespaceTokenStream& operator=(WhitespaceTokenStream&&);
    virtual ~WhitespaceTokenStream();
    virtual auto next() -> std::optional<std::string> override;
};

namespace lex = boost::spirit::lex;

using token_type =
    lex::lexertl::token<std::string_view::const_iterator, boost::mpl::vector<>, boost::mpl::false_>;
using lexer_type = lex::lexertl::actor_lexer<token_type>;

class EnglishTokenStream: public TokenStream {
    using iterator = typename lexer_type::iterator_type;

    CowString m_input;
    typename std::string_view::const_iterator m_begin{};
    typename std::string_view::const_iterator m_end{};
    iterator m_pos;
    iterator m_sentinel;

  public:
    explicit EnglishTokenStream(std::string_view input);
    explicit EnglishTokenStream(std::string input);
    explicit EnglishTokenStream(CowString input);
    EnglishTokenStream(EnglishTokenStream const&);
    EnglishTokenStream(EnglishTokenStream&&);
    EnglishTokenStream& operator=(EnglishTokenStream const&);
    EnglishTokenStream& operator=(EnglishTokenStream&&);
    virtual ~EnglishTokenStream();
    virtual auto next() -> std::optional<std::string> override;
};

/**
 * Tokenizer abstraction.
 */
class Tokenizer {
  public:
    Tokenizer();
    Tokenizer(Tokenizer const&);
    Tokenizer(Tokenizer&&);
    Tokenizer& operator=(Tokenizer const&);
    Tokenizer& operator=(Tokenizer&&);
    virtual ~Tokenizer();
    /**
     * Constructs a token stream corresponding to the given string input.
     */
    [[nodiscard]] virtual auto tokenize(std::string_view input) const
        -> std::unique_ptr<TokenStream> = 0;
    [[nodiscard]] virtual auto tokenize(std::string input) const -> std::unique_ptr<TokenStream> = 0;
    [[nodiscard]] virtual auto tokenize(CowString input) const -> std::unique_ptr<TokenStream> = 0;
};

/**
 * Splits words on any number of consecutive whitespaces.
 */
class WhitespaceTokenizer: public Tokenizer {
  public:
    WhitespaceTokenizer();
    WhitespaceTokenizer(WhitespaceTokenizer const&);
    WhitespaceTokenizer(WhitespaceTokenizer&&);
    WhitespaceTokenizer& operator=(WhitespaceTokenizer const&);
    WhitespaceTokenizer& operator=(WhitespaceTokenizer&&);
    virtual ~WhitespaceTokenizer();

    [[nodiscard]] virtual auto tokenize(std::string_view input) const
        -> std::unique_ptr<TokenStream> override;
    [[nodiscard]] virtual auto tokenize(std::string input) const
        -> std::unique_ptr<TokenStream> override;
    [[nodiscard]] virtual auto tokenize(CowString input) const
        -> std::unique_ptr<TokenStream> override;
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
  public:
    EnglishTokenizer();
    EnglishTokenizer(EnglishTokenizer const&);
    EnglishTokenizer(EnglishTokenizer&&);
    EnglishTokenizer& operator=(EnglishTokenizer const&);
    EnglishTokenizer& operator=(EnglishTokenizer&&);
    virtual ~EnglishTokenizer();

    [[nodiscard]] virtual auto tokenize(std::string_view input) const
        -> std::unique_ptr<TokenStream> override;
    [[nodiscard]] virtual auto tokenize(std::string input) const
        -> std::unique_ptr<TokenStream> override;
    [[nodiscard]] virtual auto tokenize(CowString input) const
        -> std::unique_ptr<TokenStream> override;
};

}  // namespace pisa
