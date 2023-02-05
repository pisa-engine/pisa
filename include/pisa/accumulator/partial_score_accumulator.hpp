// Copyright 2023 PISA developers
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// clang-format off

#pragma once

#ifdef PISA_ENABLE_CONCEPTS

#include <concepts>
#include <cstdint>
#include <iterator>
#include <vector>

#include "topk_queue.hpp"

namespace pisa {

/**
 * Accumulator capable of accumulating partial scores. One document can be accumulated multiple
 * times, and the scores will be summed. Typically used for term-at-a-time (TAAT) processing.
 */
template <typename T>
concept PartialScoreAccumulator = requires(T a, std::uint32_t docid, float score)
{
    /**
     * Resets the accumulator. After a reset, it is ready to be used for the next query.
     */
    a.reset();

    /**
     * Accumulates a partial score for the given document.
     */
    a.accumulate(docid, score);
}
&& requires(T const a, float score, pisa::topk_queue& topk)
{
    /**
     * Pushes results to the top-k priority queue.
     */
    a.collect(topk);
    { a.size() } -> std::same_as<std::size_t>;
};

};  // namespace pisa

// clang-format on

#endif
