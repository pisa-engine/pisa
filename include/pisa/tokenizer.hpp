#pragma once

#include <cctype>
#include <memory>
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
 * Token stream that splits text into words using ICU's `icu::BreakIterator`.
 *
 * ICU is an *optional* build-time dependency (see `PISA_ENABLE_ICU` in CMake). This header
 * intentionally includes no ICU headers: all ICU state lives in a pimpl defined in
 * `tokenizer.cpp`, so including this file never requires ICU.
 *
 * When PISA is built without ICU, the constructors throw `std::runtime_error`.
 * 
 * Copying is not supported: streams are handed out as `std::unique_ptr<TokenStream>`, so it is
 * not needed, and a correct copy would have to re-bind the break iterator to the copied text.
 *
 * Tokens are case-folded (Unicode full case folding), and whitespace and punctuation are skipped.
 */
class InternationalTokenStream: public TokenStream {
  public:
    explicit InternationalTokenStream(std::string_view input);
    explicit InternationalTokenStream(std::string input);
    explicit InternationalTokenStream(CowString input);
    InternationalTokenStream(InternationalTokenStream const&) = delete;
    InternationalTokenStream(InternationalTokenStream&&) noexcept;
    InternationalTokenStream& operator=(InternationalTokenStream const&) = delete;
    InternationalTokenStream& operator=(InternationalTokenStream&&) noexcept;
    virtual ~InternationalTokenStream();
    virtual auto next() -> std::optional<std::string> override;

  private:
    struct Impl;
    std::unique_ptr<Impl> m_impl;
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

/**
 * International tokenizer.
 *
 * Uses icu::BreakIterator to split text into words. This tokenizer is suitable for languages
 * that do not use whitespace to separate words, such as Chinese, Japanese, and Thai.
 * Also supports mixed language text.
 *
 * Requires PISA to be built with ICU (optional dependency, `PISA_ENABLE_ICU`, on by default and
 * auto-detected). Without ICU, `tokenize` throws `std::runtime_error`. The macro `PISA_HAS_ICU`
 * is defined when ICU support is compiled in.
 */
class InternationalTokenizer: public Tokenizer {
  public:
    InternationalTokenizer();
    InternationalTokenizer(InternationalTokenizer const&);
    InternationalTokenizer(InternationalTokenizer&&);
    InternationalTokenizer& operator=(InternationalTokenizer const&);
    InternationalTokenizer& operator=(InternationalTokenizer&&);
    virtual ~InternationalTokenizer();

    [[nodiscard]] virtual auto tokenize(std::string_view input) const
        -> std::unique_ptr<TokenStream> override;
    [[nodiscard]] virtual auto tokenize(std::string input) const
        -> std::unique_ptr<TokenStream> override;
    [[nodiscard]] virtual auto tokenize(CowString input) const
        -> std::unique_ptr<TokenStream> override;
};

}  // namespace pisa
