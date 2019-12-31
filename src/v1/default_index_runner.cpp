#include "v1/default_index_runner.hpp"

namespace pisa::v1 {

//[[nodiscard]] auto index_runner(IndexMetadata metadata)
//{
//    return index_runner(
//        std::move(metadata),
//        std::make_tuple(RawReader<DocId>{},
//                        DocumentBlockedReader<::pisa::simdbp_block>{},
//                        DocumentBitSequenceReader<PartitionedSequence<>>{}),
//        std::make_tuple(RawReader<Frequency>{}, PayloadBlockedReader<::pisa::simdbp_block>{}));
//}
//
//[[nodiscard]] auto scored_index_runner(IndexMetadata metadata)
//{
//    return scored_index_runner(
//        std::move(metadata),
//        std::make_tuple(RawReader<DocId>{}, DocumentBlockedReader<::pisa::simdbp_block>{}),
//        std::make_tuple(RawReader<std::uint8_t>{}));
//}

} // namespace pisa::v1
