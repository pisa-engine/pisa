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

namespace pisa {

namespace lex = boost::spirit::lex;

static size_t const TermToken = lex::min_token_id + 1;
static size_t const AbbrToken = lex::min_token_id + 2;
static size_t const PossessiveToken = lex::min_token_id + 3;
static size_t const AnyToken = lex::min_token_id + 4;

using Token =
    lex::lexertl::token<std::string_view::const_iterator, boost::mpl::vector<>, boost::mpl::false_>;

struct Lexer : lex::lexer<lex::lexertl::actor_lexer<Token>> {
    using iterator_type = boost::transform_iterator<
        std::string (*)(Token const &tok),
        boost::filter_iterator<bool (*)(Token const &),
                               lex::lexer<lex::lexertl::actor_lexer<Token>>::iterator_type>>;
    Lexer();
};

class TermTokenizer {
   public:
    TermTokenizer(std::string_view text);
    [[nodiscard]] auto begin() -> Lexer::iterator_type;
    [[nodiscard]] auto end() -> Lexer::iterator_type;

   private:
    std::string_view text_;
    std::string_view::const_iterator first_;
    std::string_view::const_iterator last_;
    Lexer lexer_{};
};

} // namespace pisa
