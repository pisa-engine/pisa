#pragma once

#include <utility>

#include <gsl/span>
#include <tl/optional.hpp>

template <typename DocumentCursor, typename PayloadCursor>
struct DocumentPayloadCursor {
    using Document = decltype(*std::declval<DocumentCursor>());
    using Payload = decltype(*std::declval<PayloadCursor>());

    explicit constexpr DocumentPayloadCursor(DocumentCursor key_cursor,
                                             PayloadCursor payload_cursor);
    [[nodiscard]] constexpr auto operator*() const -> Document;
    [[nodiscard]] constexpr auto value() const noexcept -> tl::optional<Document>;
    [[nodiscard]] constexpr auto payload() const noexcept -> tl::optional<Payload>;
    constexpr void step();
    constexpr void step_to_position(std::size_t pos);
    constexpr void step_to_geq(Document value);
    constexpr auto next() -> tl::optional<Document>;
    [[nodiscard]] constexpr auto empty() const noexcept -> bool;
    [[nodiscard]] constexpr auto position() const noexcept -> std::size_t;
    [[nodiscard]] constexpr auto size() const -> std::size_t;
    [[nodiscard]] constexpr auto sentinel() const -> Document;

   private:
    DocumentCursor m_key_cursor;
    PayloadCursor m_payload_cursor;
};

template <typename DocumentCursor, typename PayloadCursor>
constexpr DocumentPayloadCursor<DocumentCursor, PayloadCursor>::DocumentPayloadCursor(
    DocumentCursor key_cursor, PayloadCursor payload_cursor)
    : m_key_cursor(std::move(key_cursor)), m_payload_cursor(std::move(payload_cursor))
{
}

template <typename DocumentCursor, typename PayloadCursor>
[[nodiscard]] constexpr auto DocumentPayloadCursor<DocumentCursor, PayloadCursor>::operator*() const
    -> Document
{
    return *m_key_cursor;
}
template <typename DocumentCursor, typename PayloadCursor>
[[nodiscard]] constexpr auto DocumentPayloadCursor<DocumentCursor, PayloadCursor>::sentinel() const
    -> Document
{
    return m_key_cursor.sentinel();
}

template <typename DocumentCursor, typename PayloadCursor>
[[nodiscard]] constexpr auto DocumentPayloadCursor<DocumentCursor, PayloadCursor>::value() const
    noexcept -> tl::optional<Document>
{
    return m_key_cursor.value();
}

template <typename DocumentCursor, typename PayloadCursor>
[[nodiscard]] constexpr auto DocumentPayloadCursor<DocumentCursor, PayloadCursor>::payload() const
    noexcept -> tl::optional<Payload>
{
    return m_payload_cursor.value();
}

template <typename DocumentCursor, typename PayloadCursor>
constexpr void DocumentPayloadCursor<DocumentCursor, PayloadCursor>::step()
{
    m_key_cursor.step();
    m_payload_cursor.step();
}

template <typename DocumentCursor, typename PayloadCursor>
constexpr void DocumentPayloadCursor<DocumentCursor, PayloadCursor>::step_to_position(
    std::size_t pos)
{
    m_key_cursor.step_to_position(pos);
    m_payload_cursor.step_to_position(pos);
}

template <typename DocumentCursor, typename PayloadCursor>
constexpr void DocumentPayloadCursor<DocumentCursor, PayloadCursor>::step_to_geq(Document value)
{
    m_key_cursor.step_to_geq(value);
    m_payload_cursor.step_to_position(m_key_cursor.position());
}

template <typename DocumentCursor, typename PayloadCursor>
constexpr auto DocumentPayloadCursor<DocumentCursor, PayloadCursor>::next()
    -> tl::optional<Document>
{
    return m_key_cursor.next();
}

template <typename DocumentCursor, typename PayloadCursor>
[[nodiscard]] constexpr auto DocumentPayloadCursor<DocumentCursor, PayloadCursor>::empty() const
    noexcept -> bool
{
    return m_key_cursor.empty();
}

template <typename DocumentCursor, typename PayloadCursor>
[[nodiscard]] constexpr auto DocumentPayloadCursor<DocumentCursor, PayloadCursor>::position() const
    noexcept -> std::size_t
{
    return m_key_cursor.position();
}

template <typename DocumentCursor, typename PayloadCursor>
[[nodiscard]] constexpr auto DocumentPayloadCursor<DocumentCursor, PayloadCursor>::size() const
    -> std::size_t
{
    return m_key_cursor.size();
}
