#include "codec/block_codec.hpp"
#include "codec/block_codec_registry.hpp"
#define CATCH_CONFIG_MAIN
#include "catch2/catch.hpp"

#include "test_generic_sequence.hpp"

#include "block_inverted_index.hpp"

#include <algorithm>
#include <cstdlib>
#include <random>
#include <vector>

void test_block_posting_list_ops(
    pisa::BlockCodec const* codec,
    uint8_t const* data,
    uint64_t n,
    uint64_t universe,
    std::vector<std::uint32_t> const& docs,
    std::vector<std::uint32_t> const& freqs
) {
    pisa::BlockInvertedIndexCursor<> cursor(codec, data, universe, 0);
    REQUIRE(n == cursor.size());
    for (size_t i = 0; i < n; ++i, cursor.next()) {
        MY_REQUIRE_EQUAL(docs[i], cursor.docid(), "i = " << i << " size = " << n);
        MY_REQUIRE_EQUAL(freqs[i], cursor.freq(), "i = " << i << " size = " << n);
    }
    // XXX better testing of next_geq
    for (size_t i = 0; i < n; ++i) {
        cursor.reset();
        cursor.next_geq(docs[i]);
        MY_REQUIRE_EQUAL(docs[i], cursor.docid(), "i = " << i << " size = " << n);
        MY_REQUIRE_EQUAL(freqs[i], cursor.freq(), "i = " << i << " size = " << n);
    }
    cursor.reset();
    cursor.next_geq(docs.back() + 1);
    REQUIRE(universe == cursor.docid());
    cursor.reset();
    cursor.next_geq(universe);
    REQUIRE(universe == cursor.docid());
}

void random_posting_data(
    uint64_t n, uint64_t universe, std::vector<std::uint32_t>& docs, std::vector<std::uint32_t>& freqs
) {
    docs = random_sequence<std::uint32_t>(universe, n, true);
    freqs.resize(n);
    std::generate(freqs.begin(), freqs.end(), []() { return (rand() % 256) + 1; });
}

void test_block_posting_list(pisa::BlockCodecPtr codec) {
    // using posting_list_type = pisa::block_posting_list<BlockCodec>;
    uint64_t universe = 20000;
    for (size_t t = 0; t < 20; ++t) {
        double avg_gap = 1.1 + double(rand()) / RAND_MAX * 10;
        auto n = uint64_t(universe / avg_gap);

        std::vector<std::uint32_t> docs, freqs;
        random_posting_data(n, universe, docs, freqs);
        std::vector<uint8_t> data;

        pisa::index::block::write_posting_list(codec.get(), data, n, &docs[0], &freqs[0]);

        test_block_posting_list_ops(codec.get(), data.data(), n, universe, docs, freqs);
    }
}

void test_block_posting_list_reordering(pisa::BlockCodecPtr codec) {
    uint64_t universe = 20000;
    for (size_t t = 0; t < 20; ++t) {
        double avg_gap = 1.1 + double(rand()) / RAND_MAX * 10;
        auto n = uint64_t(universe / avg_gap);

        std::vector<std::uint32_t> docs, freqs;
        random_posting_data(n, universe, docs, freqs);
        std::vector<uint8_t> data;
        pisa::index::block::write_posting_list(codec.get(), data, n, &docs[0], &freqs[0]);

        // reorder blocks
        pisa::BlockInvertedIndexCursor<> cursor(codec.get(), data.data(), universe, 0);
        auto blocks = cursor.get_blocks();
        std::shuffle(
            blocks.begin() + 1,
            blocks.end(),
            std::mt19937(std::random_device()())
        );  // leave first block in place

        std::vector<uint8_t> reordered_data;
        pisa::index::block::write_posting_list(codec.get(), reordered_data, n, &docs[0], &freqs[0]);

        test_block_posting_list_ops(codec.get(), reordered_data.data(), n, universe, docs, freqs);
    }
}

TEST_CASE("block_posting_list") {
    auto codec_name = GENERATE(
        "block_optpfor",
        "block_varintg8iu",
        "block_streamvbyte",
        "block_maskedvbyte",
        "block_interpolative",
        "block_qmx",
        "block_varintgb",
        "block_simple8b",
        "block_simple16",
        "block_simdb"
    );
    auto codec = pisa::get_block_codec(codec_name);
    test_block_posting_list(codec);
}

TEST_CASE("block_posting_list_reordering") {
    auto codec_name = GENERATE(
        "block_optpfor",
        "block_varintg8iu",
        "block_streamvbyte",
        "block_maskedvbyte",
        "block_interpolative",
        "block_qmx",
        "block_varintgb",
        "block_simple8b",
        "block_simple16",
        "block_simdb"
    );
    auto codec = pisa::get_block_codec(codec_name);
    test_block_posting_list_reordering(codec);
}
