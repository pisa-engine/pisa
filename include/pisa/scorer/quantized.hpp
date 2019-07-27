#pragma once

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <utility>

#include "index_scorer.hpp"
namespace pisa {

template <typename Wand>
struct quantized : public index_scorer<Wand> {

    using index_scorer<Wand>::index_scorer;
    auto term_scorer(uint64_t term_id) const
    {
        auto s = [&](uint32_t doc, uint32_t freq) {
            return freq;
        };
        return s;
    }
};

template <typename Wand>
struct scorer_traits<quantized<Wand>> {
    using term_scorer = decltype(std::declval<quantized<Wand>>().term_scorer(0));
};

} // namespace pisa
