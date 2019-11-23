#pragma once

#include <algorithm>

#include <gsl/span>

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

template <typename T>
void partition_by_index(gsl::span<T> range, gsl::span<std::size_t> right_indices)
{
    if (right_indices.empty()) {
        return;
    }
    std::sort(right_indices.begin(), right_indices.end());
    if (right_indices[right_indices.size() - 1] >= range.size()) {
        throw std::logic_error("Essential index too large");
    }
    auto left = 0;
    auto right = range.size() - 1;
    auto eidx = 0;
    while (left < right && eidx < right_indices.size()) {
        if (left < right_indices[eidx]) {
            left += 1;
        } else {
            std::swap(range[left], range[right]);
            right -= 1;
            eidx += 1;
        }
    }
}

} // namespace pisa::v1
