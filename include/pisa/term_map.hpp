#pragma once

#include <cstdint>
#include <string>
#include <string_view>

#include "payload_vector.hpp"

namespace pisa {

class TermMap {
  public:
    TermMap();
    TermMap(TermMap const&);
    TermMap(TermMap&&);
    TermMap& operator=(TermMap const&);
    TermMap& operator=(TermMap&&);
    virtual ~TermMap();

    [[nodiscard]] virtual auto operator()(std::string_view term) -> std::optional<std::uint32_t> = 0;
    [[nodiscard]] virtual auto operator()(std::string const& term)
        -> std::optional<std::uint32_t> = 0;
};

/**
 * Maps string representations of numbers to their numeric representations.
 */
class IntMap final: public TermMap {
  public:
    [[nodiscard]] virtual auto operator()(std::string_view term)
        -> std::optional<std::uint32_t> override;
    [[nodiscard]] virtual auto operator()(std::string const& term)
        -> std::optional<std::uint32_t> override;
};

class LexiconMap final: public TermMap {
    std::optional<Payload_Vector_Buffer> m_buffer;
    Payload_Vector<std::string_view> m_lexicon;

  public:
    explicit LexiconMap(std::string const& file);
    explicit LexiconMap(Payload_Vector<std::string_view> lexicon);

    [[nodiscard]] virtual auto operator()(std::string_view term)
        -> std::optional<std::uint32_t> override;
    [[nodiscard]] virtual auto operator()(std::string const& term)
        -> std::optional<std::uint32_t> override;
};

}  // namespace pisa
