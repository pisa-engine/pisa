#define CATCH_CONFIG_MAIN

#include <catch2/catch.hpp>
#include <functional>

#include "accumulator/lazy_accumulator.hpp"
#include "cursor/block_max_scored_cursor.hpp"
#include "cursor/max_scored_cursor.hpp"
#include "cursor/scored_cursor.hpp"
#include "index_types.hpp"
#include "io.hpp"
#include "pisa_config.hpp"
#include "query/algorithm.hpp"
#include "temporary_directory.hpp"
#include "test_common.hpp"

using namespace pisa;

// NOLINTNEXTLINE(hicpp-explicit-conversions)
TEST_CASE("Stream builder for block index", "[index]")
{
    using index_type = block_simdbp_index;

    binary_freq_collection collection(PISA_SOURCE_DIR "/test/test_data/test_collection");
    pisa::TemporaryDirectory tmp;
    auto expected_path = tmp.path() / "expected";
    auto actual_path = tmp.path() / "actual";

    // Build the non-streaming way
    typename index_type::builder builder(collection.num_docs(), global_parameters{});
    for (auto const& plist: collection) {
        uint64_t freqs_sum = std::accumulate(plist.freqs.begin(), plist.freqs.end(), uint64_t(0));
        builder.add_posting_list(
            plist.docs.size(), plist.docs.begin(), plist.freqs.begin(), freqs_sum);
    }
    index_type index;
    builder.build(index);
    mapper::freeze(index, expected_path.c_str());

    // Build the streaming way
    typename index_type::stream_builder sbuilder(collection.num_docs(), global_parameters{});
    for (auto const& plist: collection) {
        uint64_t freqs_sum = std::accumulate(plist.freqs.begin(), plist.freqs.end(), uint64_t(0));
        sbuilder.add_posting_list(
            plist.docs.size(), plist.docs.begin(), plist.freqs.begin(), freqs_sum);
    }
    sbuilder.build(actual_path.string());

    auto expected_bytes = io::load_data(expected_path.string());
    auto actual_bytes = io::load_data(actual_path.string());
    CHECK(expected_bytes.size() == actual_bytes.size());
    REQUIRE(expected_bytes == actual_bytes);
}
