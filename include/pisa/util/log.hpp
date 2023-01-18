#pragma once

#include <array>
#include <cmath>
#include <limits>

namespace pisa {

template <std::size_t N>
class Log2 {
    static_assert(N >= 1, "number of precomputed values must be positive");

  public:
    constexpr Log2()
    {
        m_values[0] = -std::numeric_limits<double>::infinity();
        for (std::size_t n = 1; n < N; ++n) {
            m_values[n] = std::log2(n);
        }
    }
    constexpr double operator()(std::size_t n) const
    {
        if (n >= m_values.size()) {
            return std::log2(n);
        }
        return m_values[n];
    }

  private:
    std::array<double, N> m_values{};
};

}  // namespace pisa
