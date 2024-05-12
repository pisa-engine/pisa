#include "codec/block_codec.hpp"
#include "codec/block_codec_registry.hpp"
#define CATCH_CONFIG_MAIN
#include "catch2/catch.hpp"

#include <array>
#include <cstdlib>
#include <rapidcheck.h>
#include <type_traits>
#include <vector>

#include "codec/block_codec.hpp"

using namespace rc;

void test_case(pisa::BlockCodec const* codec, std::vector<std::uint32_t> values, bool use_sum_of_values) {
    std::uint32_t sum_of_values =
        use_sum_of_values ? std::accumulate(values.begin(), values.end(), 0) : std::uint32_t(-1);

    std::vector<uint8_t> encoded;
    codec->encode(values.data(), sum_of_values, values.size(), encoded);

    // Needed for QMX, see `include/pisa/codec/qmx.hpp` for more details.
    if (codec->get_name() == "block_qms") {
        std::array<char, 15> padding{};
        encoded.insert(encoded.end(), padding.begin(), padding.end());
    }

    std::vector<uint32_t> decoded(values.size());
    uint8_t const* out = codec->decode(encoded.data(), decoded.data(), sum_of_values, values.size());

    if (codec->get_name() == "block_qms") {
        REQUIRE(encoded.size() == out - encoded.data() + 15);
    } else {
        REQUIRE(encoded.size() == out - encoded.data());
    }
    REQUIRE(values == decoded);
}

void test_block_codec(pisa::BlockCodec const* codec) {
    const auto lengths =
        gen::elementOf(std::vector<std::size_t>{1, 2, codec->block_size() - 1, codec->block_size()});
    const auto genlist = gen::mapcat(lengths, [](std::size_t len) {
        return gen::container<std::vector<std::uint32_t>>(
            len, gen::inRange<std::uint32_t>(1, 1 << 12)
        );
    });

    bool use_sum_of_values = GENERATE(true, false);

    rc::check([&]() { test_case(codec, *genlist, use_sum_of_values); });
}

TEST_CASE("Example test case", "[codec]") {
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
    bool use_sum_of_values = GENERATE(true, false);
    std::vector<std::uint32_t> values{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,  1, 1, 1, 1, 1,
                                      1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,  1, 1, 1, 1, 1,
                                      1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,  1, 1, 1, 1, 1,
                                      1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,  1, 1, 1, 1, 1,
                                      1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,  1, 1, 1, 1, 1,
                                      1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,  1, 1, 1, 1, 1,
                                      1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 259};
    test_case(codec.get(), values, use_sum_of_values);
}

TEST_CASE("Property test", "[codec]") {
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
    std::size_t use_sum_of_values = GENERATE(true, false);
    test_block_codec(codec.get());
}
