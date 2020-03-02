#pragma once
#include <gsl/gsl_assert>
#include <cmath>

namespace pisa {

struct LinearQuantizer {
    explicit LinearQuantizer(float max, uint8_t bits) : m_max(max), m_scale(static_cast<float>(1 << (bits)) / max) {
       Expects(bits <= 32);
    }
    [[nodiscard]] auto operator()(float value) const -> std::uint32_t {
       Expects(value <= m_max);
        return std::ceil(value * m_scale);
    }
  private:
    float m_max;
    float m_scale;
};

} // namespace pisa