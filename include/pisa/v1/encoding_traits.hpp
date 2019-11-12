#pragma once

#include "codec/simdbp.hpp"
#include "v1/types.hpp"

namespace pisa::v1 {

struct SimdBPTag {
    [[nodiscard]] static auto encoding() -> std::uint32_t { return EncodingId::SimdBP; }
};

template <>
struct encoding_traits<::pisa::simdbp_block> {
    using encoding_tag = SimdBPTag;
};

} // namespace pisa::v1
