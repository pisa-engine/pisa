/* Copyright 2023 PISA developers

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License. */

#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <vector>

#include "concepts.hpp"
#include "partial_score_accumulator.hpp"
#include "topk_queue.hpp"

namespace pisa {

/**
 * Simple accumulator is an array of scores, where element n is the score of the n-th document.
 * Each reset sets all values to 0, and accumulating is done by simply adding the given score to
 * the score in the accumulator.
 */
class SimpleAccumulator: public std::vector<float> {
  public:
    explicit SimpleAccumulator(std::size_t size) : std::vector<float>(size) {
        PISA_ASSERT_CONCEPT(PartialScoreAccumulator<decltype(*this)>);
    }

    void reset() { std::fill(begin(), end(), 0.0); }

    void accumulate(std::uint32_t doc, float score) { operator[](doc) += score; }

    void collect(topk_queue& topk) {
        std::uint32_t docid = 0U;
        std::for_each(begin(), end(), [&](auto score) {
            if (topk.would_enter(score)) {
                topk.insert(score, docid);
            }
            docid += 1;
        });
    }
};

}  // namespace pisa
