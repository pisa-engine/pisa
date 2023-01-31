#pragma once

#include <cctype>
#include <optional>
#include <string>
#include <vector>

namespace pisa {

class TokenStream;

/**
 * C++ style iterator wrapping a tokenizer.
 */
class TokenIterator {
    TokenStream* m_tokenizer;
    std::size_t m_pos;
    std::optional<std::string> m_token;

  public:
    using iterator_category = std::input_iterator_tag;
    using value_type = std::string;
    using difference_type = std::ptrdiff_t;
    using pointer = value_type*;
    using reference = value_type const&;

    explicit TokenIterator(TokenStream* tokenizer);

    [[nodiscard]] auto operator*() const -> value_type;
    auto operator++() -> TokenIterator&;
    [[nodiscard]] auto operator++(int) -> TokenIterator;
    [[nodiscard]] auto operator==(TokenIterator const&) const -> bool;
    [[nodiscard]] auto operator!=(TokenIterator const&) const -> bool;
};

/**
 * Token stream abstraction. Typically takes an input string and produces consecutive string tokens.
 */
class TokenStream {
  public:
    TokenStream();
    TokenStream(TokenStream const&);
    TokenStream(TokenStream&&);
    TokenStream& operator=(TokenStream const&);
    TokenStream& operator=(TokenStream&&);
    virtual ~TokenStream();

    /**
     * Returns the next token or `std::nullopt` if no more tokens are available.
     */
    virtual auto next() -> std::optional<std::string> = 0;

    /**
     * Returns the iterator pointing to the beginning of the token stream.
     *
     * Note that this method should be called only once, as any iterator consumes processed tokens.
     */
    [[nodiscard]] virtual auto begin() -> TokenIterator;

    /**
     * Returns the iterator pointing to the end of the token stream.
     */
    [[nodiscard]] virtual auto end() -> TokenIterator;

    /** Collects all tokens into a vector. */
    [[nodiscard]] auto collect() -> std::vector<std::string>;
};

class EmptyTokenStream: public TokenStream {
  public:
    auto next() -> std::optional<std::string> override;
};

class SingleTokenStream: public TokenStream {
    std::optional<std::string> m_next;

  public:
    explicit SingleTokenStream(std::string token);
    auto next() -> std::optional<std::string> override;
};

}  // namespace pisa
