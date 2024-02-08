#pragma once

#include <cstdint>

#include <fmt/format.h>
#include <gsl/gsl_assert>

namespace pisa {

struct LinearQuantizer {
    LinearQuantizer(float max, std::uint8_t bits);
    [[nodiscard]] auto operator()(float value) const -> std::uint32_t;
    [[nodiscard]] auto range() const noexcept -> std::uint32_t;

  private:
    std::uint32_t m_range;
    float m_max;
    float m_scale;
};

}  // namespace pisa
