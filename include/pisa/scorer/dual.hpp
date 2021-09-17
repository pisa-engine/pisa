#pragma once

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <utility>

#include "index_scorer.hpp"
namespace pisa {

/* This scorer implements a 'dual' quantization mode. The idea
 * is to use 16 bits (unsigned) to store the BM25 quantized
 * score, and 16 bits to store a secondary quantized score.
 */

template <typename Wand>
struct dual: public index_scorer<Wand> {
    using index_scorer<Wand>::index_scorer;
    term_scorer_t term_scorer(uint64_t term_id) const
    {
        // XXX: Unclear how portable this is...
        auto s = [](uint32_t doc, uint32_t freq, bool bm25_score = true) { 
            //std::cerr << "bm25: " << (freq & 0xFFFFul) << "\n";
            //std::cerr << "other: " << ((freq >> 16) & (0xFFFFul)) << "\n";
            if (bm25_score) {
                return (freq & 0xFFFFul);
            } return (freq >> 16) & (0xFFFFul);
        };
        
        return s;
    }
};

}  // namespace pisa
