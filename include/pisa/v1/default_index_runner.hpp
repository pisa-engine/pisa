#pragma once

#include "index_types.hpp"
#include "v1/blocked_cursor.hpp"
#include "v1/index.hpp"
#include "v1/index_metadata.hpp"
#include "v1/raw_cursor.hpp"

namespace pisa::v1 {

[[nodiscard]] inline auto index_runner(IndexMetadata metadata)
{
    return index_runner(
        std::move(metadata),
        std::make_tuple(RawReader<DocId>{}, DocumentBlockedReader<::pisa::simdbp_block>{}),
        std::make_tuple(RawReader<Frequency>{}, PayloadBlockedReader<::pisa::simdbp_block>{}));
}

[[nodiscard]] inline auto scored_index_runner(IndexMetadata metadata)
{
    return scored_index_runner(
        std::move(metadata),
        std::make_tuple(RawReader<DocId>{}, DocumentBlockedReader<::pisa::simdbp_block>{}),
        std::make_tuple(RawReader<std::uint8_t>{}));
}

} // namespace pisa::v1
