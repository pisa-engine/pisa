#pragma once
#include <vector>
#include <cstddef>
#include <algorithm>

#include "topk_queue.hpp"

namespace pisa {

struct Simple_Accumulator : public std::vector<float> {
    Simple_Accumulator(std::ptrdiff_t size) : std::vector<float>(size) {}
    void init() { init(0u, size()); }
    void init(std::uint32_t first, std::uint32_t last)
    {
        std::fill(std::next(begin(), first), std::next(begin(), last), 0.0);
    }
    void accumulate(uint32_t doc, float score) { operator[](doc) += score; }
    void aggregate(topk_queue &topk) { aggregate(topk, 0u, size()); }
    void aggregate(topk_queue &topk, std::uint32_t first, std::uint32_t last)
    {
        uint64_t docid = 0u;
        std::for_each(std::next(begin(), first), std::next(begin(), last), [&](auto score) {
            if (topk.would_enter(score)) {
                topk.insert(score, docid);
            }
            docid += 1;
        });
    }
};

} // namespace pisa
