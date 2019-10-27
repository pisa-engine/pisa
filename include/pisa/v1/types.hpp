#pragma once

#include <cstdint>
#include <functional>

namespace pisa::v1 {

using TermId = std::uint32_t;
using DocId = std::uint32_t;
using Frequency = std::uint32_t;
using Score = float;
using Result = std::pair<DocId, Score>;

} // namespace pisa::v1
