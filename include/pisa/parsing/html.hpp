#pragma once

#include <string>
#include <string_view>

#include "gumbo.h"

namespace pisa::parsing::html {

[[nodiscard]] auto cleantext(std::string_view html) -> std::string;

}  // namespace pisa::parsing::html
