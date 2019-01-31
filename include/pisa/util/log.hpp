#pragma once

#include <cmath>
#include <array>

#include <gsl/gsl>
#include <range/v3/view/iota.hpp>

namespace pisa {

template <size_t N>
class Log2 {
    static_assert(N >= 0, "number of precomputed values must be non-negative");

   public:
    constexpr Log2() noexcept
    {
        for (auto n : ranges::view::ints(size_t(0), N)) {
            gsl::at(m_values, n) = std::log2(n);
        }
    }
    constexpr double operator()(size_t n) const
    {
        if (n >= m_values.size()) {
            return std::log2(n);
        }
        return gsl::at(m_values, n);
    }

   private:
    std::array<double, N> m_values{};
};

} // namespace pisa
