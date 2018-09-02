#pragma once

namespace ds2i {

template <size_t N>
class Log2 {
    static_assert(N >= 0, "number of precomputed values must be non-negative");

   public:
    constexpr Log2() {
        for (size_t n = 0; n < N; ++n) {
            m_values[n] = std::log2(n);
        }
    }
    constexpr double operator()(size_t n) const {
        if (n >= m_values.size()) {
            return std::log2(n);
        }
        return m_values[n];
    }

   private:
    std::array<double, N> m_values{};
};

} // namespace ds2i
