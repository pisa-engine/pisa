#include <cmath>
#include <stdexcept>

#include "fmt/core.h"
#include "linear_quantizer.hpp"

namespace pisa {

LinearQuantizer::LinearQuantizer(float max, std::uint8_t bits)
    : m_range((1U << bits) - 1U), m_max(max), m_scale(static_cast<float>(m_range - 1) / max) {
    if (max <= 0.0) {
        throw std::runtime_error(
            fmt::format("Max score for linear quantizer must be positive but {} passed", max)
        );
    }
    if (bits > 32 or bits < 2) {
        throw std::runtime_error(fmt::format(
            "Linear quantizer must take a number of bits between 2 and 32 but {} passed", bits
        ));
    }
}

auto LinearQuantizer::operator()(float value) const -> std::uint32_t {
    if (value < 0 || value > m_max) {
        throw std::invalid_argument(
            fmt::format("quantized value must be between 0 and {} but {} given", m_max, value)
        );
    }
    return std::round(value * m_scale) + 1;
}

auto LinearQuantizer::range() const noexcept -> std::uint32_t {
    return m_range;
}

}  // namespace pisa
