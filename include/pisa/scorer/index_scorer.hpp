#pragma once

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <functional>

namespace pisa {

template <class Iter>
struct scorer_traits;

using term_scorer_t = std::function<float(uint32_t, uint32_t)>;

template <typename Wand>
struct index_scorer {
   protected:
    const Wand &m_wdata;

   public:
    explicit index_scorer(const Wand &wdata) : m_wdata(wdata) {}
};

} // namespace pisa
