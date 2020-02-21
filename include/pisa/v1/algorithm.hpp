#pragma once

#include <algorithm>
#include <vector>

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
    std::vector<T> essential;
    essential.reserve(right_indices.size());
    std::vector<T> non_essential;
    non_essential.reserve(range.size() - right_indices.size());

    auto cidx = 0;
    auto eidx = 0;
    while (eidx < right_indices.size()) {
        if (cidx < right_indices[eidx]) {
            non_essential.push_back(std::move(range[cidx]));
            cidx += 1;
        } else {
            essential.push_back(std::move(range[cidx]));
            eidx += 1;
            cidx += 1;
        }
    }
    std::move(std::next(range.begin(), cidx), range.end(), std::back_inserter(non_essential));
    auto pos = std::move(non_essential.begin(), non_essential.end(), range.begin());
    std::move(essential.begin(), essential.end(), pos);
}

} // namespace pisa::v1
