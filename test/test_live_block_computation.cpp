#define CATCH_CONFIG_MAIN
#include "bit_vector.hpp"
#include "catch2/catch.hpp"
#include <rapidcheck.h>

#include "query/live_block_computation.hpp"

using namespace pisa;

TEST_CASE("test_live_block_computation")
{
    rc::check([](std::vector<uint16_t> scores, uint16_t threshold) {
        {
            auto bv = compute_live_quant16({scores}, threshold);
            REQUIRE(bv.size() == scores.size());

            bit_vector::enumerator en(bv, 0);
            for (auto&& s: scores) {
                bool condition = s >= threshold;
                REQUIRE(en.next() == condition);
            }
        }
    });
}
#ifdef __AVX__

TEST_CASE("test_avx_live_block_computation")
{
    rc::check([](std::vector<uint16_t> scores, uint16_t threshold) {
        {
            auto bv = compute_live_quant16({scores}, threshold);
            auto avx_bv = avx_compute_live_quant16({scores}, threshold);
            REQUIRE(bv.size() == avx_bv.size());

            bit_vector::enumerator en(bv, 0);
            bit_vector::enumerator avx_en(avx_bv, 0);
            while (en.position() < bv.size()) {
                REQUIRE(en.next() == avx_en.next());
            }
        }
    });
}

#endif

#ifdef __AVX2__

TEST_CASE("test_avx2_live_block_computation")
{
    rc::check([](std::vector<uint16_t> scores, uint16_t threshold) {
        {
            auto bv = compute_live_quant16({scores}, threshold);
            auto avx2_bv = avx2_compute_live_quant16({scores}, threshold);
            REQUIRE(bv.size() == avx2_bv.size());

            bit_vector::enumerator en(bv, 0);
            bit_vector::enumerator avx2_en(avx2_bv, 0);
            while (en.position() < bv.size()) {
                REQUIRE(en.next() == avx2_en.next());
            }
        }
    });
}

#endif
