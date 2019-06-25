#pragma once

#include <algorithm>
#include <cmath>
#include <cstdint>

namespace pisa {

template <typename Wand>
struct scorer {
   protected:
    const Wand &m_wdata;
   public:
    explicit scorer(const Wand &wdata) : m_wdata(wdata) {}

    virtual std::function<float(uint32_t, uint32_t)> operator()(uint64_t term_id) const = 0;
};

} // namespace pisa
