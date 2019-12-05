#pragma once

#include <utility>

#include <gsl/span>

namespace pisa::v1 {

template <typename Cursor, typename TransformFn>
struct TransformCursor {
    using Value =
        decltype(std::declval<TransformFn>(std::declval<std::add_lvalue_reference_t<Cursor>>()));

    constexpr TransformCursor(Cursor cursor, TransformFn transform)
        : m_cursor(std::move(cursor)), m_transform(std::move(transform))
    {
    }

    [[nodiscard]] constexpr auto operator*() const -> Value { return value(); }
    [[nodiscard]] constexpr auto value() const noexcept -> Value
    {
        return m_transform(m_cursor.value());
    }
    constexpr void advance() { m_cursor.advance(); }
    constexpr void advance_to_position(std::size_t pos) { m_cursor.advance_to_position(pos); }
    [[nodiscard]] constexpr auto empty() const noexcept -> bool { return m_cursor.empty(); }
    [[nodiscard]] constexpr auto position() const noexcept -> std::size_t
    {
        return m_cursor.position();
    }
    [[nodiscard]] constexpr auto size() const -> std::size_t { return m_cursor.size(); }

   private:
    Cursor m_cursor;
    TransformFn m_transform;
};

template <typename Cursor, typename TransformFn>
auto transform(Cursor cursor, TransformFn transform)
{
    return TransformCursor<Cursor, TransformFn>(std::move(cursor), std::move(transform));
}

template <typename Cursor, typename TransformFn>
struct TransformPayloadCursor {
    constexpr TransformPayloadCursor(Cursor cursor, TransformFn transform)
        : m_cursor(std::move(cursor)), m_transform(std::move(transform))
    {
    }

    [[nodiscard]] constexpr auto operator*() const { return value(); }
    [[nodiscard]] constexpr auto value() const noexcept { return m_cursor.value(); }
    [[nodiscard]] constexpr auto payload() { return m_transform(m_cursor); }
    constexpr void advance() { m_cursor.advance(); }
    constexpr void advance_to_position(std::size_t pos) { m_cursor.advance_to_position(pos); }
    [[nodiscard]] constexpr auto empty() const noexcept -> bool { return m_cursor.empty(); }
    [[nodiscard]] constexpr auto position() const noexcept -> std::size_t
    {
        return m_cursor.position();
    }
    [[nodiscard]] constexpr auto size() const -> std::size_t { return m_cursor.size(); }
    [[nodiscard]] constexpr auto sentinel() const { return m_cursor.sentinel(); }

   private:
    Cursor m_cursor;
    TransformFn m_transform;
};

template <typename Cursor, typename TransformFn>
auto transform_payload(Cursor cursor, TransformFn transform)
{
    return TransformPayloadCursor<Cursor, TransformFn>(std::move(cursor), std::move(transform));
}

} // namespace pisa::v1
