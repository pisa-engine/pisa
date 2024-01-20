#include "pisa/term_map.hpp"

#include <charconv>
#include <stdexcept>
#include <string>

#include <fmt/format.h>

namespace pisa {

TermMap::TermMap() = default;
TermMap::TermMap(TermMap const&) = default;
TermMap::TermMap(TermMap&&) = default;
TermMap& TermMap::operator=(TermMap const&) = default;
TermMap& TermMap::operator=(TermMap&&) = default;
TermMap::~TermMap() = default;

auto IntMap::find(std::string_view term) const -> std::optional<std::uint32_t> {
    std::uint32_t value;
    auto [ptr, ec] = std::from_chars(term.begin(), term.end(), value, 10);
    if (ec == std::errc::result_out_of_range || ec == std::errc::invalid_argument
        || ptr != term.end()) {
        throw std::invalid_argument(fmt::format("unable to parse integer: {}", term));
    }
    return value;
}

auto IntMap::find(std::string const& term) const -> std::optional<std::uint32_t> {
    return this->find(std::string_view(term));
}

LexiconMap::LexiconMap(std::string const& file)
    : m_buffer(Payload_Vector_Buffer::from_file(file)), m_lexicon(*m_buffer) {}

LexiconMap::LexiconMap(Payload_Vector<std::string_view> lexicon)
    : m_buffer(std::nullopt), m_lexicon(lexicon) {}

auto LexiconMap::find(std::string_view term) const -> std::optional<std::uint32_t> {
    return pisa::binary_search(m_lexicon.begin(), m_lexicon.end(), term);
}

auto LexiconMap::find(std::string const& term) const -> std::optional<std::uint32_t> {
    return pisa::binary_search(m_lexicon.begin(), m_lexicon.end(), term);
}

auto LexiconMap::operator[](std::uint32_t term_id) const -> std::string_view {
    return m_lexicon[term_id];
}

auto LexiconMap::size() const noexcept -> std::size_t {
    return m_lexicon.size();
}

}  // namespace pisa
