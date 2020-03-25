#pragma once
#include <algorithm>
#include <cstddef>
#include <vector>

#include "topk_queue.hpp"

namespace pisa {

struct Simple_Accumulator: public std::vector<float> {
    explicit Simple_Accumulator(std::ptrdiff_t size) : std::vector<float>(size) {}
    void init() { std::fill(begin(), end(), 0.0); }
    void accumulate(uint32_t doc, float score) { operator[](doc) += score; }
    void aggregate(topk_queue& topk)
    {
        uint64_t docid = 0U;
        std::for_each(begin(), end(), [&](auto score) {
            if (topk.would_enter(score)) {
                topk.insert(score, docid);
            }
            docid += 1;
        });
    }
};

}  // namespace pisa
