#include "v1/blocked_cursor.hpp"

namespace pisa::v1 {

[[nodiscard]] auto BaseBlockedCursor::block_offset(size_type block) const -> offset_type
{
    return block > 0U ? m_block_endpoints[block - 1] : static_cast<offset_type>(0U);
}
[[nodiscard]] auto BaseBlockedCursor::decoded_block() -> value_type*
{
    return m_decoded_block.data();
}
[[nodiscard]] auto BaseBlockedCursor::encoded_block(offset_type offset) -> uint8_t const*
{
    return std::next(reinterpret_cast<std::uint8_t const*>(m_encoded_blocks.data()), offset);
}
[[nodiscard]] auto BaseBlockedCursor::length() const -> size_type { return m_length; }
[[nodiscard]] auto BaseBlockedCursor::num_blocks() const -> size_type { return m_num_blocks; }

[[nodiscard]] auto BaseBlockedCursor::operator*() const -> value_type { return m_current_value; }
[[nodiscard]] auto BaseBlockedCursor::value() const noexcept -> value_type { return *(*this); }
[[nodiscard]] auto BaseBlockedCursor::empty() const noexcept -> bool
{
    return value() >= sentinel();
}
[[nodiscard]] auto BaseBlockedCursor::position() const noexcept -> std::size_t
{
    return m_current_block.number * m_block_length + m_current_block.offset;
}
[[nodiscard]] auto BaseBlockedCursor::size() const -> std::size_t { return m_length; }
[[nodiscard]] auto BaseBlockedCursor::sentinel() const -> value_type
{
    return std::numeric_limits<value_type>::max();
}
[[nodiscard]] auto BaseBlockedCursor::current_block() -> Block& { return m_current_block; }
[[nodiscard]] auto BaseBlockedCursor::decoded_value(size_type n) -> value_type
{
    return m_decoded_block[n] + 1U;
}

void BaseBlockedCursor::update_current_value(value_type val) { m_current_value = val; }
void BaseBlockedCursor::increase_current_value(value_type val) { m_current_value += val; }

} // namespace pisa::v1
