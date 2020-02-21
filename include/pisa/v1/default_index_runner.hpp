#pragma once

#include "index_types.hpp"
#include "v1/bit_sequence_cursor.hpp"
#include "v1/blocked_cursor.hpp"
#include "v1/index.hpp"
#include "v1/index_metadata.hpp"
#include "v1/raw_cursor.hpp"
#include "v1/sequence/partitioned_sequence.hpp"
#include "v1/sequence/positive_sequence.hpp"

namespace pisa::v1 {

[[nodiscard]] inline auto index_runner(IndexMetadata metadata)
{
    return index_runner(std::move(metadata),
                        std::make_tuple(RawReader<DocId>{},
                                        DocumentBlockedReader<::pisa::simdbp_block>{},
                                        DocumentBitSequenceReader<IndexedSequence>{},
                                        DocumentBitSequenceReader<PartitionedSequence<>>{}),
                        std::make_tuple(RawReader<Frequency>{},
                                        PayloadBlockedReader<::pisa::simdbp_block>{},
                                        PayloadBitSequenceReader<PositiveSequence<>>{}));
}

[[nodiscard]] inline auto scored_index_runner(IndexMetadata metadata)
{
    return scored_index_runner(std::move(metadata),
                               std::make_tuple(RawReader<DocId>{},
                                               DocumentBlockedReader<::pisa::simdbp_block>{},
                                               DocumentBitSequenceReader<IndexedSequence>{},
                                               DocumentBitSequenceReader<PartitionedSequence<>>{}),
                               std::make_tuple(RawReader<std::uint8_t>{}));
}

} // namespace pisa::v1
