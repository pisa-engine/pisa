#pragma once

#include <algorithm>
#include <cctype>
#include <string>

#include <boost/config/warning_disable.hpp>
#include <boost/iterator/filter_iterator.hpp>
#include <boost/iterator/transform_iterator.hpp>
#include <boost/spirit/include/lex_lexertl.hpp>
#include <boost/spirit/include/phoenix_operator.hpp>
#include <boost/spirit/include/qi.hpp>
#include <boost/tokenizer.hpp>

namespace pisa {

namespace lex = boost::spirit::lex;

static size_t TermToken = lex::min_token_id + 1;
static size_t AbbrToken = lex::min_token_id + 2;

template <typename Lexer>
struct tokens : lex::lexer<Lexer> {
    tokens()
    {
        word = lex::token_def<>("[a-zA-Z0-9]+('[a-zA-Z]+)?", TermToken);
        this->self =
            lex::token_def<>(".") | lex::token_def<>("([a-zA-Z]+\\.){2,}", AbbrToken) | word;
    }

    lex::token_def<> word;
};

class TermTokenizer {
   public:
    using token_type = lex::lexertl::token<char const *, boost::mpl::vector<>, boost::mpl::false_>;
    using lexer_type = lex::lexertl::actor_lexer<token_type>;

    TermTokenizer(std::string const &text) : text_(text) {
    }

    [[nodiscard]] auto begin() {
        first = text_.c_str();
        last = &first[text_.size()];
        return boost::make_transform_iterator(
            boost::make_filter_iterator(pred, lexer_.begin(first, last)), transform);
    }

    [[nodiscard]] auto end() {
        return boost::make_transform_iterator(boost::make_filter_iterator(pred, lexer_.end()),
                                              transform);
    }

   private:
    static bool pred(token_type const &tok)
    {
        return tok.id() == TermToken || tok.id() == AbbrToken;
    }
    static std::string transform(token_type const &tok)
    {
        return std::string(tok.value().begin(), tok.value().end());
    }

    std::string const &text_;
    char const *first = nullptr;
    char const *last = nullptr;
    tokens<lexer_type> lexer_{};
};

}
