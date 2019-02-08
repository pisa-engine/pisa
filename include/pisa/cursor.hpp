#pragma once

#include <limits>

namespace pisa::cursor {

struct Sentinel {
} end;

static constexpr uint32_t document_bound = std::numeric_limits<uint32_t>::max();

} // namespace pisa::cursor
