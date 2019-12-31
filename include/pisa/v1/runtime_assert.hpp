#pragma once

#include <stdexcept>
#include <type_traits>

namespace pisa::v1 {

template <typename Message>
inline void runtime_assert(bool condition, Message&& message)
{
    if (not condition) {
        if constexpr (std::is_invocable_r_v<std::string, Message>) {
            throw std::runtime_error(message());
        } else {
            throw std::runtime_error(std::forward<Message>(message));
        }
    }
}

} // namespace pisa::v1
