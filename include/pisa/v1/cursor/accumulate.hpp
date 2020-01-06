#include "v1/cursor/for_each.hpp"

namespace pisa::v1 {

template <typename Cursor, typename Payload, typename AccumulateFn>
[[nodiscard]] constexpr inline auto accumulate(Cursor cursor, Payload init, AccumulateFn accumulate)
{
    for_each(cursor, [&](auto&& cursor) { init = accumulate(init, cursor); });
    return init;
}

} // namespace pisa::v1
