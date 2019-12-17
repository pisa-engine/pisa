#pragma once

#include <string>

#include "v1/index_metadata.hpp"

namespace pisa::v1 {

auto score_index(IndexMetadata meta, std::size_t threads) -> IndexMetadata;

} // namespace pisa::v1
