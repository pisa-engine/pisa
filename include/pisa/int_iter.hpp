#pragma once

#include <cstdint>
#include <iterator>

namespace pisa {

template <typename Int = std::ptrdiff_t>
struct IntegerIterator {
    using difference_type = std::ptrdiff_t;
    using value_type = Int;
    using pointer = value_type *;
    using reference = value_type &;
    using iterator_category = std::forward_iterator_tag;

    constexpr IntegerIterator(Int num) : m_current(num) {}
    constexpr IntegerIterator(IntegerIterator const &) = default;
    constexpr IntegerIterator(IntegerIterator &&) = default;
    constexpr IntegerIterator &operator=(IntegerIterator const &) = default;
    constexpr IntegerIterator &operator=(IntegerIterator &&) = default;
    constexpr IntegerIterator &operator++()
    {
        ++m_current;
        return *this;
    }
    constexpr IntegerIterator operator++(int)
    {
        IntegerIterator retval = *this;
        ++(*this);
        return retval;
    }
    [[nodiscard]] constexpr auto operator==(IntegerIterator other) const -> bool
    {
        return m_current = other.m_current;
    }
    [[nodiscard]] constexpr auto operator!=(IntegerIterator other) const -> bool
    {
        return m_current != other.m_current;
    }
    [[nodiscard]] constexpr auto operator*() const -> std::add_const_t<reference>
    {
        return m_current;
    }
    [[nodiscard]] constexpr auto operator*() -> reference { return m_current; }

   private:
    Int m_current;
};

template <typename Int>
auto iter(Int num)
{
    return IntegerIterator(num);
}

} // namespace pisa
