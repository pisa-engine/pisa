#pragma once

#include <memory>

#include "text_filter.hpp"
#include "token_filter.hpp"
#include "token_stream.hpp"
#include "tokenizer.hpp"

namespace pisa {

class TextAnalyzer {
    std::unique_ptr<Tokenizer> m_tokenizer;
    std::vector<std::unique_ptr<TextFilter>> m_text_filters;
    std::vector<std::unique_ptr<TokenFilter>> m_token_filters;

  public:
    explicit TextAnalyzer(std::unique_ptr<Tokenizer> tokenizer);

    void add_text_filter(std::unique_ptr<TextFilter> text_filter);
    void add_token_filter(std::unique_ptr<TokenFilter> token_filter);

    template <typename T, typename... Args>
    void emplace_text_filter(Args... args) {
        m_text_filters.emplace_back(std::make_unique<T>(args...));
    }

    template <typename T, typename... Args>
    void emplace_token_filter(Args... args) {
        m_token_filters.emplace_back(std::make_unique<T>(args...));
    }

    [[nodiscard]] auto analyze(std::string_view input) const -> std::unique_ptr<TokenStream>;
};

}  // namespace pisa
