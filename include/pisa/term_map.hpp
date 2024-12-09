#pragma once

#include <cstdint>
#include <string>
#include <string_view>

#include "concepts/container.hpp"
#include "concepts/mapping.hpp"
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

    [[nodiscard]] virtual auto find(std::string_view term) const -> std::optional<std::uint32_t> = 0;
    [[nodiscard]] virtual auto find(std::string const& term) const
        -> std::optional<std::uint32_t> = 0;
};

/**
 * Maps string representations of numbers to their numeric representations.
 */
class IntMap final: public TermMap {
  public:
    [[nodiscard]] virtual auto find(std::string_view term) const
        -> std::optional<std::uint32_t> override;
    [[nodiscard]] virtual auto find(std::string const& term) const
        -> std::optional<std::uint32_t> override;
};

static_assert(concepts::ReverseMapping<IntMap, std::string_view>);

class LexiconMap final: public TermMap {
    std::optional<Payload_Vector_Buffer> m_buffer;
    Payload_Vector<std::string_view> m_lexicon;

  public:
    explicit LexiconMap(std::string const& file);
    explicit LexiconMap(Payload_Vector<std::string_view> lexicon);

    [[nodiscard]] auto operator[](std::uint32_t term_id) const -> std::string_view;

    [[nodiscard]] virtual auto find(std::string_view term) const
        -> std::optional<std::uint32_t> override;
    [[nodiscard]] virtual auto find(std::string const& term) const
        -> std::optional<std::uint32_t> override;
    [[nodiscard]] auto size() const noexcept -> std::size_t;
};

static_assert(concepts::BidirectionalMapping<LexiconMap, std::string_view>);
static_assert(concepts::SizedContainer<LexiconMap>);

}  // namespace pisa
