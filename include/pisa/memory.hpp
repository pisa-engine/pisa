// Copyright 2023 PISA developers
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <cstdint>
#include <cstring>
#include <type_traits>

namespace pisa {

/**
 * A proxy object for safe reinterpret casts and assignments.
 */
template <typename T, typename Byte>
class ReinterpretProxy {
    static_assert(std::is_trivially_constructible_v<T>);
    Byte* m_ptr;
    std::size_t m_len;

  public:
    explicit ReinterpretProxy(Byte* ptr, std::size_t len = sizeof(T)) : m_ptr(ptr), m_len(len) {}

    void operator=(T const& value) { std::memcpy(m_ptr, &value, m_len); }

    [[nodiscard]] auto operator*() const -> T {
        T dst{0};
        std::memcpy(&dst, m_ptr, m_len);
        return dst;
    }
};

/**
 * Safe version of `reinterpret_cast<T>`.
 *
 * Returns a proxy that performs a memcpy on assignment, thus bypasses alignment issues
 * and does not introduce an undefined behavior.
 *
 * Example usage:
 *
 * bitwise_reinterpret<std::uint32_t>(byte_ptr) = 789;
 *
 * It will copy 4 bytes representing 789 to the memory location starting at `byte_ptr`.
 */
template <typename T>
auto bitwise_reinterpret(std::uint8_t* dst) -> ReinterpretProxy<T, std::uint8_t> {
    return ReinterpretProxy<T, std::uint8_t>{dst, sizeof(T)};
}

/**
 * Safe (const) version of `reinterpret_cast<T>`.
 *
 * Returns a proxy that performs a memcpy on dereference.
 *
 * Example usage:
 *
 * auto n = bitwise_reinterpret<std::uint32_t>(byte_ptr);
 *
 * It will copy 4 bytes from the memory location starting at `byte_ptr` interpreting them
 * as `uint32_t`.
 *
 * It is possible to copy fewer bytes than the size of the returned type by passing `len`.
 */
template <typename T>
auto bitwise_reinterpret(std::uint8_t const* dst, std::size_t len = sizeof(T))
    -> ReinterpretProxy<T, std::uint8_t const> {
    return ReinterpretProxy<T, std::uint8_t const>{dst, len};
}

}  // namespace pisa
