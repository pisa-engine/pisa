#pragma once

#include "codec/simdbp.hpp"
#include "v1/sequence/indexed_sequence.hpp"
#include "v1/sequence/partitioned_sequence.hpp"
#include "v1/sequence/positive_sequence.hpp"
#include "v1/types.hpp"

namespace pisa::v1 {

struct SimdBPTag {
    [[nodiscard]] static auto encoding() -> std::uint32_t { return EncodingId::SimdBP; }
};

template <>
struct encoding_traits<::pisa::simdbp_block> {
    using encoding_tag = SimdBPTag;
};

struct PartitionedSequenceTag {
    [[nodiscard]] static auto encoding() -> std::uint32_t { return EncodingId::PEF; }
};

template <>
struct encoding_traits<PartitionedSequence<>> {
    using encoding_tag = PartitionedSequenceTag;
};

struct IndexedSequenceTag {
    [[nodiscard]] static auto encoding() -> std::uint32_t { return 17U; }
};

template <>
struct encoding_traits<IndexedSequence> {
    using encoding_tag = IndexedSequenceTag;
};

struct PositiveSequenceTag {
    [[nodiscard]] static auto encoding() -> std::uint32_t { return EncodingId::PositiveSeq; }
};

template <>
struct encoding_traits<PositiveSequence<>> {
    using encoding_tag = PositiveSequenceTag;
};

} // namespace pisa::v1
