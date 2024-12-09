// Copyright 2024 PISA developers
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

#include <concepts>
#include <cstdint>

#include "container.hpp"
#include "type_alias.hpp"

namespace pisa::concepts {

/**
 * A posting cursor iterates over a posting list.
 */
template <typename C>
concept PostingCursor = SizedContainer<C> && requires(C const &cursor)
{
    /** Returns the document ID at the current position. */
    { cursor.docid() } -> std::convertible_to<std::uint32_t>;
} && requires(C cursor) {
    /** Moves the cursor to the next position. */
    cursor.next();
};

/**
 * A posting cursor returning a score.
 */
template <typename C>
concept FrequencyPostingCursor = PostingCursor<C>  && requires(C cursor) {
    /** Returns the value of the payload. */
    { cursor.freq() } -> std::convertible_to<std::uint32_t>;
};

/**
 * A posting cursor returning a score.
 */
template <typename C>
concept ScoredPostingCursor = PostingCursor<C>  && requires(C cursor) {
    /** Returns the value of the payload. */
    { cursor.score() } -> std::convertible_to<Score>;
};

/**
 * A cursor over a posting list that stores postings in increasing order of document IDs.
 */
template <typename C>
concept SortedPostingCursor = PostingCursor<C>
&& requires(C cursor, std::uint32_t docid) {
    /**
     * Moves the cursor to the next position at which the document ID is at least `docid`.
     * If the current ID already satisfies this condition, the cursor will not move. It will
     * never move backwards.
     */
    cursor.next_geq(docid);
};

/**
 * A posting cursor with max score.
 */
template <typename C>
concept MaxScorePostingCursor = ScoredPostingCursor<C> && requires(C const& cursor) {
    /** Returns the max score of the entire list. */
    { cursor.max_score() } noexcept -> std::convertible_to<Score>;
};

/**
 * A posting cursor with block-max scores.
 */
template <typename C>
concept BlockMaxPostingCursor = MaxScorePostingCursor<C> && SortedPostingCursor<C>
&& requires(C cursor) {
    /** Returns the max highest docid of the current block. */
    { cursor.block_max_docid() } -> std::convertible_to<DocId>;
    /** Returns the max score of the current block. */
    { cursor.block_max_score() } -> std::convertible_to<Score>;
};

};  // namespace pisa

// clang-format on
