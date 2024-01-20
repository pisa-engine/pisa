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

#ifdef PISA_ENABLE_CONCEPTS

#include <concepts>
#include <cstdint>
#include <optional>

namespace pisa::concepts {

/**
 * Mapping from an integer to a payload value.
 *
 * One of the examples is a mapping from document ID to document title or URL.
 */
template <typename T, typename Payload>
concept Mapping = requires(T const map, std::uint32_t pos) {
    /** Get payload at position `pos`. */
    { map[pos] } -> std::convertible_to<Payload>;

    /** Returns the number of posting lists in the index. */
    { map.size() }  noexcept -> std::convertible_to<std::size_t>;
};

/**
 * Mapping from a payload value to ordinal ID.
 */
template <typename T, typename Payload>
concept ReverseMapping = requires(T const map, Payload payload) {
    /** Get the position of the given payload. */
    { map.find(payload) } -> std::convertible_to<std::optional<std::uint32_t>>;
};

/**
 * Mapping from an integer to a payload value and back.
 *
 * One of the examples is a term lexicon, which maps from term IDs to terms and back.
 * The backwards mapping can be used to look up term IDs after parsing a query to term tokens.
 */
template <typename T, typename Payload>
concept BidirectionalMapping = Mapping<T, Payload> && ReverseMapping<T, Payload>;

};  // namespace pisa

// clang-format on

#endif
