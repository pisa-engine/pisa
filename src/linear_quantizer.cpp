#include <cassert>
#include <cmath>
#include <stdexcept>

#include "fmt/core.h"
#include "linear_quantizer.hpp"

namespace pisa {

[[nodiscard]] auto all_ones(std::uint32_t bits) -> std::uint32_t {
    if (bits > 32 or bits < 2) {
        throw std::runtime_error(fmt::format(
            "Linear quantizer must take a number of bits between 2 and 32 but {} passed", bits
        ));
    }
    auto half = std::uint32_t(1) << (bits - 1);
    return half - 1 + half;
}

LinearQuantizer::LinearQuantizer(float max, std::uint8_t bits)
    : m_range(all_ones(bits)), m_max(max) {
    if (max <= 0.0) {
        throw std::runtime_error(
            fmt::format("Max score for linear quantizer must be positive but {} passed", max)
        );
    }
}

auto LinearQuantizer::operator()(float value) const -> std::uint32_t {
    if (value < 0 || value > m_max) {
        throw std::invalid_argument(
            fmt::format("quantized value must be between 0 and {} but {} given", m_max, value)
        );
    }
    auto normalized_value = static_cast<double>(value / m_max);
    return static_cast<std::uint32_t>(normalized_value * (m_range - 1)) + 1;
}

auto LinearQuantizer::range() const noexcept -> std::uint32_t {
    return m_range;
}

}  // namespace pisa
