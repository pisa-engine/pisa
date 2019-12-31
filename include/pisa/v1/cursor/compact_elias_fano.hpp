#include <cstdint>
#include <limits>

#include "util/likely.hpp"

namespace pisa::v1 {

struct CompactEliasFanoCursor {
    using value_type = std::uint32_t;

    /// Dereferences the current value.
    //[[nodiscard]] auto operator*() const -> value_type
    //{
    //    if (PISA_UNLIKELY(empty())) {
    //        return sentinel();
    //    }
    //    return bit_cast<T>(gsl::as_bytes(m_bytes.subspan(m_current, sizeof(T))));
    //}

    ///// Alias for `operator*()`.
    //[[nodiscard]] auto value() const noexcept -> value_type { return *(*this); }

    ///// Advances the cursor to the next position.
    // constexpr void advance() { m_current += sizeof(T); }

    ///// Moves the cursor to the position `pos`.
    // constexpr void advance_to_position(std::size_t pos) { m_current = pos * sizeof(T); }

    ///// Moves the cursor to the next value equal or greater than `value`.
    // constexpr void advance_to_geq(T value)
    //{
    //    while (this->value() < value) {
    //        advance();
    //    }
    //}

    ///// Returns `true` if there is no elements left.
    //[[nodiscard]] constexpr auto empty() const noexcept -> bool
    //{
    //    return m_current == m_bytes.size();
    //}

    ///// Returns the current position.
    //[[nodiscard]] constexpr auto position() const noexcept -> std::size_t
    //{
    //    return m_current / sizeof(T);
    //}

    ///// Returns the number of elements in the list.
    //[[nodiscard]] constexpr auto size() const -> std::size_t { return m_bytes.size() / sizeof(T);
    //}

    [[nodiscard]] constexpr auto sentinel() const -> value_type
    {
        return std::numeric_limits<value_type>::max();
    }

   private:
    // BitVector const* m_bv;
    // offsets m_of;

    std::size_t m_position;
    value_type m_value;
    // BitVector::unary_enumerator m_high_enumerator;
};

} // namespace pisa::v1
