#define CATCH_CONFIG_MAIN
#include "catch2/catch.hpp"

#include <array>
#include <cstdlib>
#include <limits>
#include <rapidcheck.h>
#include <type_traits>
#include <vector>

#include "codec/block_codecs.hpp"
#include "codec/maskedvbyte.hpp"
#include "codec/qmx.hpp"
#include "codec/simdbp.hpp"
#include "codec/simple16.hpp"
#include "codec/simple8b.hpp"
#include "codec/streamvbyte.hpp"
#include "codec/varintgb.hpp"

#include "test_common.hpp"

using namespace rc;

template <typename BlockCodec>
void test_case(std::vector<std::uint32_t> values, bool use_sum_of_values) {
    std::uint32_t sum_of_values =
        use_sum_of_values ? std::accumulate(values.begin(), values.end(), 0) : std::uint32_t(-1);

    std::vector<uint8_t> encoded;
    BlockCodec::encode(values.data(), sum_of_values, values.size(), encoded);

    // Needed for QMX, see `include/pisa/codec/qmx.hpp` for more details.
    if constexpr (std::is_same_v<BlockCodec, pisa::qmx_block>) {
        std::array<char, 15> padding{};
        encoded.insert(encoded.end(), padding.begin(), padding.end());
    }

    std::vector<uint32_t> decoded(values.size());
    uint8_t const* out =
        BlockCodec::decode(encoded.data(), decoded.data(), sum_of_values, values.size());

    if constexpr (std::is_same_v<BlockCodec, pisa::qmx_block>) {
        REQUIRE(encoded.size() == out - encoded.data() + 15);
    } else {
        REQUIRE(encoded.size() == out - encoded.data());
    }
    REQUIRE(values == decoded);
}

template <typename BlockCodec>
void test_block_codec() {
    const auto lengths = gen::elementOf(
        std::vector<std::size_t>{1, 2, BlockCodec::block_size - 1, BlockCodec::block_size}
    );
    const auto genlist = gen::mapcat(lengths, [](std::size_t len) {
        return gen::container<std::vector<std::uint32_t>>(
            len, gen::inRange<std::uint32_t>(1, 1 << 12)
        );
    });

    std::size_t use_sum_of_values = GENERATE(true, false);

    rc::check([&]() { test_case<BlockCodec>(*genlist, use_sum_of_values); });
}

// NOLINTNEXTLINE(hicpp-explicit-conversions)
TEMPLATE_TEST_CASE(
    "Example test case",
    "[codec]",
    pisa::optpfor_block,
    pisa::varint_G8IU_block,
    pisa::streamvbyte_block,
    pisa::maskedvbyte_block,
    pisa::interpolative_block,
    pisa::qmx_block,
    pisa::varintgb_block,
    pisa::simple8b_block,
    pisa::simple16_block,
    pisa::simdbp_block
) {
    std::size_t use_sum_of_values = GENERATE(true, false);
    std::vector<std::uint32_t> values{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,  1, 1, 1, 1, 1,
                                      1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,  1, 1, 1, 1, 1,
                                      1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,  1, 1, 1, 1, 1,
                                      1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,  1, 1, 1, 1, 1,
                                      1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,  1, 1, 1, 1, 1,
                                      1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,  1, 1, 1, 1, 1,
                                      1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 259};
    test_case<TestType>(values, use_sum_of_values);
}

// NOLINTNEXTLINE(hicpp-explicit-conversions)
TEMPLATE_TEST_CASE(
    "Property test",
    "[codec]",
    pisa::optpfor_block,
    pisa::varint_G8IU_block,
    pisa::streamvbyte_block,
    pisa::maskedvbyte_block,
    pisa::interpolative_block,
    pisa::qmx_block,
    pisa::varintgb_block,
    pisa::simple8b_block,
    pisa::simple16_block,
    pisa::simdbp_block
) {
    std::size_t use_sum_of_values = GENERATE(true, false);
    test_block_codec<TestType>();
}
