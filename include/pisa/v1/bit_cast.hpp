#pragma once

#include <cstring>

#include <gsl/span>

namespace pisa::v1 {

template <class T>
constexpr auto bit_cast(gsl::span<const std::byte> mem) -> std::remove_const_t<T>
{
    std::remove_const_t<T> dst{};
    std::memcpy(&dst, mem.data(), sizeof(T));
    return dst;
}

} // namespace pisa::v1
