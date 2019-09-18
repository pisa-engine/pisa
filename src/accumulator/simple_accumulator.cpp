#include <algorithm>
#include <vector>

#include "accumulator/simple_accumulator.hpp"
#include "topk_queue.hpp"

namespace pisa {

SimpleAccumulator::SimpleAccumulator(std::ptrdiff_t size) : std::vector<float>(size) {}

void SimpleAccumulator::init() { std::fill(begin(), end(), 0.0); }

void SimpleAccumulator::aggregate(TopKQueue &topk)
{
    std::uint64_t docid = 0u;
    std::for_each(begin(), end(), [&](auto score) {
        if (topk.would_enter(score)) {
            topk.insert(score, docid);
        }
        docid += 1;
    });
}

}
