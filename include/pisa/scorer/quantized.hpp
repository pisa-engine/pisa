#pragma once

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <utility>

#include "index_scorer.hpp"
namespace pisa {

template <typename Wand>
struct quantized: public index_scorer<Wand> {
    using index_scorer<Wand>::index_scorer;
    term_scorer_t term_scorer([[maybe_unused]] uint64_t term_id) const
    {
        return []([[maybe_unused]] uint32_t doc, uint32_t freq) { return freq; };
    }
};

}  // namespace pisa
