#include "pisa/text_analyzer.hpp"
#include "pisa/cow_string.hpp"
#include "pisa/token_stream.hpp"

namespace pisa {

TextAnalyzer::TextAnalyzer(std::unique_ptr<Tokenizer> tokenizer) : m_tokenizer(std::move(tokenizer))
{}

class FlatMapStream: public TokenStream {
    std::unique_ptr<TokenStream> m_input_stream;
    std::unique_ptr<TokenStream> m_inner_stream = nullptr;
    TokenFilter const& m_map;

  public:
    explicit FlatMapStream(std::unique_ptr<TokenStream> input_stream, TokenFilter& map)
        : m_input_stream(std::move(input_stream)), m_map(map)
    {}

    auto next() -> std::optional<std::string> override
    {
        std::optional<std::string> token = std::nullopt;
        while (!token) {
            if (m_inner_stream == nullptr || !(token = m_inner_stream->next())) {
                token = m_input_stream->next();
                if (!token) {
                    break;
                }
                m_inner_stream = m_map.filter(std::move(*token));
                token = m_inner_stream->next();
            }
        }
        return token;
    }
};

void TextAnalyzer::add_text_filter(std::unique_ptr<TextFilter> text_filter)
{
    m_text_filters.emplace_back(std::move(text_filter));
}

void TextAnalyzer::add_token_filter(std::unique_ptr<TokenFilter> token_filter)
{
    m_token_filters.emplace_back(std::move(token_filter));
}

auto TextAnalyzer::analyze(std::string_view input) const -> std::unique_ptr<TokenStream>
{
    CowString text(input);
    for (auto& text_filter: m_text_filters) {
        text = CowString(text_filter->filter(text.as_view()));
    }
    auto stream = m_tokenizer->tokenize(std::move(text));
    for (auto& token_filter: m_token_filters) {
        stream = std::make_unique<FlatMapStream>(std::move(stream), *token_filter);
    }
    return stream;
}

}  // namespace pisa
