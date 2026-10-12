#include <cstdlib>
#include <exception>
#include <vector>

#include "catch2/catch_all.hpp"

#include "test_generic_sequence.hpp"

#include "codec/strict_elias_fano.hpp"

TEST_CASE("strict_elias_fano") {
    pisa::global_parameters params;

    uint64_t n = 10000;
    auto universe = uint64_t(2 * n);
    auto seq = random_sequence(universe, n, true);

    test_sequence(pisa::strict_elias_fano(), params, universe, seq);
}
