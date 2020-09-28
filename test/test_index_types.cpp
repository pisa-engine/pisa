#define CATCH_CONFIG_MAIN
#include "catch2/catch.hpp"

//#include <algorithm>
//#include <cstdlib>
//#include <numeric>
//#include <vector>

#include "pisa/index_types.hpp"

TEST_CASE("freq_index", "[index][unit]")
{
    REQUIRE(pisa::detail::index_name(pisa::ef_index()) == "ef");
    REQUIRE(pisa::detail::index_name(pisa::single_index()) == "single");
    REQUIRE(pisa::detail::index_name(pisa::pefuniform_index()) == "pefuniform");
    REQUIRE(pisa::detail::index_name(pisa::pefopt_index()) == "pefopt");
    REQUIRE(pisa::detail::index_name(pisa::block_optpfor_index()) == "block_optpfor");
    REQUIRE(pisa::detail::index_name(pisa::block_varintg8iu_index()) == "block_varintg8iu");
    REQUIRE(pisa::detail::index_name(pisa::block_streamvbyte_index()) == "block_streamvbyte");
    REQUIRE(pisa::detail::index_name(pisa::block_maskedvbyte_index()) == "block_maskedvbyte");
    REQUIRE(pisa::detail::index_name(pisa::block_interpolative_index()) == "block_interpolative");
    REQUIRE(pisa::detail::index_name(pisa::block_qmx_index()) == "block_qmx");
    REQUIRE(pisa::detail::index_name(pisa::block_varintgb_index()) == "block_varintgb");
    REQUIRE(pisa::detail::index_name(pisa::block_simple8b_index()) == "block_simple8b");
    REQUIRE(pisa::detail::index_name(pisa::block_simple16_index()) == "block_simple16");
    REQUIRE(pisa::detail::index_name(pisa::block_simdbp_index()) == "block_simdbp");
}
