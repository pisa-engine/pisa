#pragma once
#include "spdlog/spdlog.h"
#include <cmath>
#include <gsl/gsl_assert>

namespace pisa {

struct LinearQuantizer {
    explicit LinearQuantizer(float max, uint8_t bits)
        : m_max(max), m_scale(static_cast<float>(1U << (bits)) / max)
    {
        if (bits > 32 or bits == 0) {
            throw std::runtime_error(fmt::format(
                "Linear quantizer must take a number of bits between 1 and 32 but {} passed", bits));
        }
    }
    [[nodiscard]] auto operator()(float value) const -> std::uint32_t
    {
        Expects(value <= m_max);
        return std::ceil(value * m_scale);
    }

  private:
    float m_max;
    float m_scale;
};

}  // namespace pisa