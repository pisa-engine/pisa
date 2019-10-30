#pragma once

#include <cstdint>
#include <functional>

#include <gsl/span>

#include "v1/types.hpp"

namespace pisa::v1 {

struct VectorSource {
    std::vector<std::vector<std::byte>> bytes{};
    std::vector<std::vector<std::size_t>> offsets{};
    std::vector<std::vector<std::uint32_t>> sizes{};
};

} // namespace pisa::v1
