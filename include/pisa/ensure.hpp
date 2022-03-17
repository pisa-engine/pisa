#pragma once

#include <string_view>
#include <utility>

#include <spdlog/spdlog.h>

#include "util/compiler_attribute.hpp"

namespace pisa {

class Ensure {
  public:
    explicit Ensure(bool condition) : m_condition(condition) {}

    template <typename Error>
    PISA_ALWAYSINLINE auto or_throw(Error&& error)
    {
        if (not m_condition) {
            throw std::forward<Error>(error);
        }
    }

    template <typename Fn>
    PISA_ALWAYSINLINE auto or_else(Fn&& fn)
    {
        if (not m_condition) {
            fn();
        }
    }

    PISA_ALWAYSINLINE auto or_panic(std::string_view error_msg)
    {
        if (not m_condition) {
            spdlog::error(error_msg);
            std::exit(EXIT_FAILURE);
        }
    }

    template <typename Fn>
    PISA_ALWAYSINLINE auto or_panic_with(Fn&& fn)
    {
        if (not m_condition) {
            fn();
            std::exit(EXIT_FAILURE);
        }
    }

  private:
    bool m_condition;
};

[[nodiscard]] inline auto ensure(bool condition) -> Ensure
{
    return Ensure(condition);
}

}  // namespace pisa
