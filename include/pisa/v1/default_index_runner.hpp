#pragma once

#include "index_types.hpp"
#include "v1/blocked_cursor.hpp"
#include "v1/index.hpp"
#include "v1/raw_cursor.hpp"

namespace pisa::v1 {

using DefaultIndexRunner = IndexRunner<RawReader<std::uint32_t>{},
                                       RawReader<std::uint8_t>{},
                                       RawReader<float>{},
                                       BlockedReader<::pisa::simdbp_block, true>{},
                                       BlockedReader<::pisa::simdbp_block, false>{}>;

}
