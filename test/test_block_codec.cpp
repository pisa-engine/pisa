#define CATCH_CONFIG_MAIN
#include "catch2/catch.hpp"

#include <algorithm>
#include <numeric>
#include <random>

#include "codec/block_codec.hpp"
#include "codec/maskedvbyte.hpp"
#include "codec/qmx.hpp"
#include "codec/simdbp.hpp"
#include "codec/simple16.hpp"
#include "codec/simple8b.hpp"
#include "codec/streamvbyte.hpp"
#include "codec/varintgb.hpp"

using namespace pisa;
using std::uint32_t;

TEMPLATE_TEST_CASE("block_codec",
                   "[block_codec][unit]",
                   simdbp_block,
                   qmx_block,
                   varintgb_block,
                   streamvbyte_block,
                   // TODO: simple16_block,
                   simple8b_block,
                   maskedvbyte_block)
{
    std::random_device rd;
    auto seed = rd();
    CAPTURE(seed);
    std::mt19937 gen(seed);
    std::uniform_int_distribution<uint32_t> dis(0, std::numeric_limits<uint32_t>::max());

    size_t n = 128;
    auto codec = block_codec<TestType>();
    std::vector<uint32_t> input(n);
    std::generate(input.begin(), input.end(), [&]() { return dis(gen); });
    auto sum = std::accumulate(input.begin(), input.end(), 0);

    std::vector<uint8_t> bytes;
    codec.encode(input.data(), sum, n, bytes);

    std::vector<uint32_t> output(n);
    codec.decode(bytes.data(), &output[0], sum, n);

    REQUIRE(input == output);
}
