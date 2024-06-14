#define CATCH_CONFIG_MAIN

#include <algorithm>
#include <cstdlib>
#include <vector>

#include <catch2/catch.hpp>

#include "block_inverted_index.hpp"
#include "codec/block_codec_registry.hpp"
#include "temporary_directory.hpp"
#include "test_generic_sequence.hpp"

template <typename Accumulator>
void test_block_posting_accumulator(std::string const& codec_name) {
    CAPTURE(codec_name);
    pisa::TemporaryDirectory tmpdir;

    pisa::global_parameters params;
    uint64_t universe = 20000;

    std::size_t num_docs = 30;
    auto output_filename = (tmpdir.path() / "temp.bin").string();
    auto block_codec = pisa::get_block_codec(codec_name);

    REQUIRE(block_codec != nullptr);

    Accumulator accumulator(block_codec, num_docs, output_filename);

    using vec_type = std::vector<std::uint32_t>;
    std::vector<std::pair<vec_type, vec_type>> posting_lists(30);
    for (auto& plist: posting_lists) {
        double avg_gap = 1.1 + double(rand()) / RAND_MAX * 10;
        auto n = std::uint64_t(universe / avg_gap);
        plist.first = random_sequence<std::uint32_t>(universe, n, true);
        plist.second.resize(n);
        std::generate(plist.second.begin(), plist.second.end(), []() { return (rand() % 256) + 1; });
        accumulator.accumulate_posting_list(n, &plist.first[0], &plist.second[0]);
    }
    accumulator.finish();

    {
        pisa::BlockInvertedIndex index(pisa::MemorySource::mapped_file(output_filename), block_codec);
        for (size_t i = 0; i < posting_lists.size(); ++i) {
            auto const& plist = posting_lists[i];
            auto doc_enum = index[i];
            REQUIRE(plist.first.size() == doc_enum.size());
            for (size_t p = 0; p < plist.first.size(); ++p, doc_enum.next()) {
                MY_REQUIRE_EQUAL(plist.first[p], doc_enum.docid(), "i = " << i << " p = " << p);
                MY_REQUIRE_EQUAL(plist.second[p], doc_enum.freq(), "i = " << i << " p = " << p);
            }
            REQUIRE(index.num_docs() == doc_enum.docid());
        }
    }
}

TEMPLATE_TEST_CASE(
    "block posting accumulator",
    "[block][accumulator]",
    pisa::index::block::InMemoryPostingAccumulator,
    pisa::index::block::StreamPostingAccumulator
) {
    test_block_posting_accumulator<TestType>("block_optpfor");
    test_block_posting_accumulator<TestType>("block_varintg8iu");
    test_block_posting_accumulator<TestType>("block_streamvbyte");
    test_block_posting_accumulator<TestType>("block_maskedvbyte");
    test_block_posting_accumulator<TestType>("block_varintgb");
    test_block_posting_accumulator<TestType>("block_interpolative");
    test_block_posting_accumulator<TestType>("block_qmx");
    test_block_posting_accumulator<TestType>("block_simple8b");
    test_block_posting_accumulator<TestType>("block_simple16");
    test_block_posting_accumulator<TestType>("block_simdbp");
}
