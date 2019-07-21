#pragma once

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <functional>

namespace pisa {

template <typename Wand>
struct index_scorer {
   protected:
    const Wand &m_wdata;
   public:
    explicit index_scorer(const Wand &wdata) : m_wdata(wdata) {}

    virtual std::function<float(uint32_t, uint32_t)> term_scorer(uint64_t term_id) const = 0;
};

} // namespace pisa
