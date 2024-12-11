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

namespace pisa::concepts {

/**
 * Any container with a size.
 */
template <typename T>
concept SizedContainer = requires(T const container) {
    /** Returns the number of posting lists in the index. */
    { container.size() }  noexcept -> std::convertible_to<std::size_t>;
};

};  // namespace pisa

// clang-format on
