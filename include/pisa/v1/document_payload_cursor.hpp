#pragma once

#include <utility>

#include <gsl/span>

namespace pisa::v1 {

template <typename DocumentCursor, typename PayloadCursor>
struct DocumentPayloadCursor {
    using Document = decltype(*std::declval<DocumentCursor>());
    using Payload = decltype(*std::declval<PayloadCursor>());

    explicit constexpr DocumentPayloadCursor(DocumentCursor key_cursor,
                                             PayloadCursor payload_cursor)
        : m_key_cursor(std::move(key_cursor)), m_payload_cursor(std::move(payload_cursor))
    {
    }

    [[nodiscard]] constexpr auto operator*() const -> Document { return value(); }
    [[nodiscard]] constexpr auto value() const noexcept -> Document { return m_key_cursor.value(); }
    [[nodiscard]] constexpr auto payload() const noexcept -> Payload
    {
        return m_payload_cursor.value();
    }
    constexpr void advance()
    {
        m_key_cursor.advance();
        m_payload_cursor.advance();
    }
    constexpr void advance_to_position(std::size_t pos)
    {
        m_key_cursor.advance_to_position(pos);
        m_payload_cursor.advance_to_position(pos);
    }
    constexpr void advance_to_geq(Document value)
    {
        m_key_cursor.advance_to_geq(value);
        m_payload_cursor.advance_to_position(m_key_cursor.position());
    }
    [[nodiscard]] constexpr auto empty() const noexcept -> bool { return m_key_cursor.empty(); }
    [[nodiscard]] constexpr auto position() const noexcept -> std::size_t
    {
        return m_key_cursor.position();
    }
    [[nodiscard]] constexpr auto size() const -> std::size_t { return m_key_cursor.size(); }
    [[nodiscard]] constexpr auto sentinel() const -> Document { return m_key_cursor.sentinel(); }

   private:
    DocumentCursor m_key_cursor;
    PayloadCursor m_payload_cursor;
};

} // namespace pisa::v1
