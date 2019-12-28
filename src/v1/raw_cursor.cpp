#include "v1/raw_cursor.hpp"

namespace pisa::v1 {

template struct RawCursor<std::uint32_t>;
template struct RawCursor<std::uint8_t>;
template struct RawCursor<float>;
template struct RawReader<std::uint32_t>;
template struct RawReader<std::uint8_t>;
template struct RawReader<float>;
template struct RawWriter<std::uint32_t>;
template struct RawWriter<std::uint8_t>;
template struct RawWriter<float>;
template struct CursorTraits<RawCursor<std::uint32_t>>;
template struct CursorTraits<RawCursor<std::uint8_t>>;
template struct CursorTraits<RawCursor<float>>;

} // namespace pisa::v1
