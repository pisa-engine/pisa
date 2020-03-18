#define CATCH_CONFIG_MAIN
#include "catch2/catch.hpp"

#include "test_generic_sequence.hpp"

#include "sequence/strict_sequence.hpp"
#include "sequence/uniform_partitioned_sequence.hpp"
#include <cstdlib>
#include <vector>

TEST_CASE("uniform_partitioned_sequence")
{
    pisa::global_parameters params;
    using pisa::indexed_sequence;
    using pisa::strict_sequence;

    // test singleton sequences
    std::vector<uint64_t> short_seq;
    short_seq.push_back(0);
    test_sequence(pisa::uniform_partitioned_sequence<indexed_sequence>(), params, 1, short_seq);
    test_sequence(pisa::uniform_partitioned_sequence<strict_sequence>(), params, 1, short_seq);
    short_seq[0] = 1;
    test_sequence(pisa::uniform_partitioned_sequence<indexed_sequence>(), params, 2, short_seq);
    test_sequence(pisa::uniform_partitioned_sequence<strict_sequence>(), params, 2, short_seq);

    std::vector<double> avg_gaps = {1.1, 1.9, 2.5, 3, 4, 5, 10};
    for (auto avg_gap: avg_gaps) {
        uint64_t n = 10000;
        auto universe = uint64_t(n * avg_gap);
        auto seq = random_sequence(universe, n, true);

        test_sequence(pisa::uniform_partitioned_sequence<indexed_sequence>(), params, universe, seq);
        test_sequence(pisa::uniform_partitioned_sequence<strict_sequence>(), params, universe, seq);
    }
}
