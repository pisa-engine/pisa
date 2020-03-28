#pragma once

#include <algorithm>
#include <cctype>
#include <string>
#include <string_view>

#include <boost/config/warning_disable.hpp>
#include <boost/iterator/filter_iterator.hpp>
#include <boost/iterator/transform_iterator.hpp>
#include <boost/spirit/include/lex_lexertl.hpp>
#include <boost/spirit/include/phoenix_operator.hpp>
#include <boost/spirit/include/qi.hpp>
#include <boost/tokenizer.hpp>

namespace pisa {

namespace lex = boost::spirit::lex;

enum TokenType { Abbreviature = 1, Possessive = 2, Term = 3, NotValid = 4 };

template <typename Lexer>
struct tokens: lex::lexer<Lexer> {
    tokens()
    {
        // Note: parsing process takes the first match from left to right.
        this->self = lex::token_def<>("([a-zA-Z]+\\.){2,}", TokenType::Abbreviature)
            | lex::token_def<>("[a-zA-Z0-9]+('[a-zA-Z]+)", TokenType::Possessive)
            | lex::token_def<>("[a-zA-Z0-9]+", TokenType::Term)
            | lex::token_def<>(".", TokenType::NotValid);
    }
};

using token_type =
    lex::lexertl::token<std::string_view::const_iterator, boost::mpl::vector<>, boost::mpl::false_>;
using lexer_type = lex::lexertl::actor_lexer<token_type>;

class TermTokenizer {
  public:
    static tokens<lexer_type> const LEXER;

    explicit TermTokenizer(std::string_view text)
        : text_(text), first_(text_.begin()), last_(text_.end())
    {}

    [[nodiscard]] auto begin()
    {
        first_ = text_.begin();
        last_ = text_.end();
        return boost::make_transform_iterator(
            boost::make_filter_iterator(is_valid, LEXER.begin(first_, last_)), transform);
    }

    [[nodiscard]] auto end()
    {
        return boost::make_transform_iterator(
            boost::make_filter_iterator(is_valid, LEXER.end()), transform);
    }

  private:
    static bool is_valid(token_type const& tok) { return tok.id() != TokenType::NotValid; }

    static std::string transform(token_type const& tok)
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

    std::string_view text_;
    std::string_view::const_iterator first_;
    std::string_view::const_iterator last_;
};

}  // namespace pisa
