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

#pragma once

// clang-format off

#ifdef PISA_ENABLE_CONCEPTS

#include <concepts>
#include <cstdint>

#include "container.hpp"
#include "posting_cursor.hpp"

namespace pisa::concepts {

/**
 * Inverted index is a collection of posting lists.
 */
template <typename T, typename Cursor>
concept InvertedIndex = PostingCursor<Cursor> && SizedContainer<T>
&& requires(T const i, std::uint32_t termid) {
    /** Accesses a posting list via a cursor. */
    { i.operator[](termid) } -> std::same_as<Cursor>;

    /** Returns the number of indexed documents. */
    { i.num_docs() } noexcept -> std::convertible_to<std::size_t>;
};

/**
 * Inverted index that stores postings sorted by document IDs.
 */
template <typename T, typename Cursor>
concept SortedInvertedIndex = InvertedIndex<T, Cursor> && SortedPostingCursor<Cursor>;

};  // namespace pisa

// clang-format on

#endif
