#include "tokenizer.hpp"

namespace pisa {

Lexer::Lexer()
{
    this->self = lex::token_def<>(".", AnyToken) | lex::token_def<>("([a-zA-Z]+\\.){2,}", AbbrToken)
                 | lex::token_def<>("[a-zA-Z0-9]+", TermToken)
                 | lex::token_def<>("[a-zA-Z0-9]+('[a-zA-Z]+)", PossessiveToken);
}

[[nodiscard]] auto pred(Token const &tok) -> bool { return tok.id() != AnyToken; }

[[nodiscard]] auto transform(Token const &tok) -> std::string
{
    auto &val = tok.value();
    switch (tok.id()) {
    case AbbrToken: {
        std::string term;
        std::copy_if(
            val.begin(), val.end(), std::back_inserter(term), [](char ch) { return ch != '.'; });
        return term;
    }
    case PossessiveToken:
        return std::string(val.begin(), std::find(val.begin(), val.end(), '\''));
    default:
        return std::string(val.begin(), val.end());
    }
}

TermTokenizer::TermTokenizer(std::string_view text)
    : text_(std::move(text)), first_(text_.begin()), last_(text_.end())
{}

[[nodiscard]] auto TermTokenizer::begin() -> Lexer::iterator_type
{
    first_ = text_.begin();
    last_ = text_.end();
    return boost::make_transform_iterator(
        boost::make_filter_iterator(pred, lexer_.begin(first_, last_)), transform);
}

[[nodiscard]] auto TermTokenizer::end() -> Lexer::iterator_type
{
    return boost::make_transform_iterator(boost::make_filter_iterator(pred, lexer_.end()),
                                          transform);
}

} // namespace pisa
