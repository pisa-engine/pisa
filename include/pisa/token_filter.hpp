#pragma once

#include <memory>
#include <string_view>
#include <unordered_set>

#include <KrovetzStemmer/KrovetzStemmer.hpp>
#include <Porter2.hpp>

#include "cow_string.hpp"
#include "token_stream.hpp"

namespace pisa {

/**
 * Token filter transforms a single term into zero or more terms.
 *
 * For example:
 *  - a stemmer takes a term and returns a single term and stems it,
 *  - a stop word filter takes a term and returns it if not a stop word,
 *    or returns an empty token stream for a stop word,
 *  - a synonym filter takes a single term and possibly expands to
 *    multiple terms.
 */
class TokenFilter {
    /**
     * Returns a token stream for the given input.
     */
  public:
    TokenFilter();
    TokenFilter(TokenFilter const&);
    TokenFilter(TokenFilter&&);
    TokenFilter& operator=(TokenFilter const&);
    TokenFilter& operator=(TokenFilter&&);
    virtual ~TokenFilter();

    [[nodiscard]] virtual auto filter(std::string_view input) const
        -> std::unique_ptr<TokenStream> = 0;
    [[nodiscard]] virtual auto filter(std::string input) const -> std::unique_ptr<TokenStream> = 0;
    [[nodiscard]] virtual auto filter(CowString input) const -> std::unique_ptr<TokenStream> = 0;
};

class Porter2Stemmer final: public TokenFilter {
  public:
    [[nodiscard]] auto filter(std::string_view input) const -> std::unique_ptr<TokenStream> override;
    [[nodiscard]] auto filter(std::string input) const -> std::unique_ptr<TokenStream> override;
    [[nodiscard]] auto filter(CowString input) const -> std::unique_ptr<TokenStream> override;
};

class KrovetzStemmer final: public TokenFilter {
    std::shared_ptr<stem::KrovetzStemmer> m_stemmer = std::make_shared<stem::KrovetzStemmer>();

  public:
    [[nodiscard]] auto filter(std::string_view input) const -> std::unique_ptr<TokenStream> override;
    [[nodiscard]] auto filter(std::string input) const -> std::unique_ptr<TokenStream> override;
    [[nodiscard]] auto filter(CowString input) const -> std::unique_ptr<TokenStream> override;
};

class LowercaseFilter final: public TokenFilter {
  public:
    [[nodiscard]] auto filter(std::string_view input) const -> std::unique_ptr<TokenStream> override;
    [[nodiscard]] auto filter(std::string input) const -> std::unique_ptr<TokenStream> override;
    [[nodiscard]] auto filter(CowString input) const -> std::unique_ptr<TokenStream> override;
};

class StopWordRemover final: public TokenFilter {
    std::unordered_set<std::string> m_stopwords;

  public:
    explicit StopWordRemover(std::unordered_set<std::string> stopwords);
    [[nodiscard]] auto filter(std::string_view input) const -> std::unique_ptr<TokenStream> override;
    [[nodiscard]] auto filter(std::string input) const -> std::unique_ptr<TokenStream> override;
    [[nodiscard]] auto filter(CowString input) const -> std::unique_ptr<TokenStream> override;
};

}  // namespace pisa
