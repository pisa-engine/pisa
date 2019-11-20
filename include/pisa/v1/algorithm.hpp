#pragma once

#include <algorithm>

namespace pisa::v1 {

template <typename Container>
[[nodiscard]] auto min_value(Container&& cursors)
{
    auto pos =
        std::min_element(cursors.begin(), cursors.end(), [](auto const& lhs, auto const& rhs) {
            return lhs.value() < rhs.value();
        });
    return pos->value();
}

template <typename Container>
[[nodiscard]] auto min_sentinel(Container&& cursors)
{
    auto pos =
        std::min_element(cursors.begin(), cursors.end(), [](auto const& lhs, auto const& rhs) {
            return lhs.sentinel() < rhs.sentinel();
        });
    return pos->sentinel();
}

} // namespace pisa::v1
