#define BOOST_TEST_MODULE indexed_sequence

#include "test_generic_sequence.hpp"

#include "indexed_sequence.hpp"
#include <vector>
#include <cstdlib>

BOOST_AUTO_TEST_CASE(indexed_sequence)
{
    ds2i::global_parameters params;

    std::vector<double> avg_gaps = { 1.1, 1.9, 2.5, 3, 4, 5, 10 };
    for (auto avg_gap: avg_gaps) {
        uint64_t n = 10000;
        uint64_t universe = uint64_t(n * avg_gap);
        auto seq = random_sequence(universe, n, true);

        test_sequence(ds2i::indexed_sequence(), params, universe, seq);
    }
}
