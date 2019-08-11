#pragma once

#include <cstddef>
#include <vector>

namespace pisa {

struct topk_queue;

struct SimpleAccumulator : public std::vector<float> {
    SimpleAccumulator(std::ptrdiff_t size);
    /// Initialization executed once before a query.
    void init();
    /// Accumulates a posting's partial score.
    void accumulate(std::uint32_t doc, float score);
    /// Modifies a given heap (typically empty at first) to include top results.
    void aggregate(topk_queue &topk);
};

inline void SimpleAccumulator::accumulate(std::uint32_t doc, float score)
{
    operator[](doc) += score;
}

} // namespace pisa
