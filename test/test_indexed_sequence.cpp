#define CATCH_CONFIG_MAIN
#include "catch2/catch.hpp"

#include "test_generic_sequence.hpp"

#include "sequence/indexed_sequence.hpp"
#include <cstdlib>
#include <vector>

TEST_CASE("indexed_sequence")
{
    pisa::global_parameters params;

    std::vector<double> avg_gaps = {1.1, 1.9, 2.5, 3, 4, 5, 10};
    for (auto avg_gap: avg_gaps) {
        uint64_t n = 10000;
        auto universe = uint64_t(n * avg_gap);
        auto seq = random_sequence(universe, n, true);

        test_sequence(pisa::indexed_sequence(), params, universe, seq);
    }
}
