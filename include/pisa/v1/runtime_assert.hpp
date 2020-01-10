#pragma once

#include <spdlog/spdlog.h>

#include <stdexcept>
#include <type_traits>

namespace pisa::v1 {

struct RuntimeAssert {
    explicit RuntimeAssert(bool condition) : passes_(condition) {}

    template <typename Message>
    void or_exit(Message&& message)
    {
        if (not passes_) {
            if constexpr (std::is_invocable_r_v<std::string, Message>) {
                spdlog::error(message());
            } else {
                spdlog::error("{}", message);
            }
        }
    }

    template <typename Message>
    void or_throw(Message&& message)
    {
        if (not passes_) {
            if constexpr (std::is_invocable_r_v<std::string, Message>) {
                throw std::runtime_error(message());
            } else {
                throw std::runtime_error(std::forward<Message>(message));
            }
        }
    }

   private:
    bool passes_;
};

[[nodiscard]] inline auto runtime_assert(bool condition) -> RuntimeAssert
{
    return RuntimeAssert(condition);
}

} // namespace pisa::v1
