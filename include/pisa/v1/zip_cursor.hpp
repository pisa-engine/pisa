#pragma once

#include <utility>

#include <gsl/span>

namespace pisa::v1 {

template <typename... Cursors>
struct ZipCursor {
    using Value = std::tuple<decltype(*std::declval<Cursors...>())>;

    explicit constexpr ZipCursor(Cursors... cursors) : m_cursors(std::move(cursors...)) {}

    [[nodiscard]] constexpr auto operator*() const -> Value { return value(); }
    [[nodiscard]] constexpr auto value() const noexcept -> Value
    {
        auto deref = [](auto... cursors) { return std::make_tuple(cursors.value()...); };
        return std::apply(deref, m_cursors);
    }
    constexpr void advance()
    {
        auto advance_all = [](auto... cursors) { (cursors.advance(), ...); };
        std::apply(advance_all, m_cursors);
    }
    constexpr void advance_to_position(std::size_t pos)
    {
        auto advance_all = [pos](auto... cursors) { (cursors.advance_to_position(pos), ...); };
        std::apply(advance_all, m_cursors);
    }
    //[[nodiscard]] constexpr auto empty() const noexcept -> bool { return m_key_cursor.empty(); }
    [[nodiscard]] constexpr auto position() const noexcept -> std::size_t
    {
        return std::get<0>(m_cursors).position();
    }
    //[[nodiscard]] constexpr auto size() const -> std::size_t { return m_key_cursor.size(); }
    //[[nodiscard]] constexpr auto sentinel() const -> Document { return m_key_cursor.sentinel(); }

   private:
    std::tuple<Cursors...> m_cursors;
};

} // namespace pisa::v1
