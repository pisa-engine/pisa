#define CATCH_CONFIG_MAIN
#include "catch2/catch.hpp"

#include <algorithm>
#include <cstdlib>
#include <numeric>
#include <vector>

#include "test_generic_sequence.hpp"

#include "sequence/partitioned_sequence.hpp"
#include "sequence/positive_sequence.hpp"
#include "sequence/uniform_partitioned_sequence.hpp"

template <typename BaseSequence>
void test_positive_sequence()
{
    srand(42);
    pisa::global_parameters params;
    size_t n = 50000;
    std::vector<uint64_t> values(n);
    std::generate(values.begin(), values.end(), []() { return (rand() % 256) + 1; });
    uint64_t universe = std::accumulate(values.begin(), values.end(), 0) + 1;

    using sequence_type = pisa::positive_sequence<BaseSequence>;
    pisa::bit_vector_builder bvb;
    sequence_type::write(bvb, values.begin(), universe, values.size(), params);
    pisa::bit_vector bv(&bvb);
    typename sequence_type::enumerator r(bv, 0, universe, values.size(), params);

    for (size_t i = 0; i < n; ++i) {
        auto val = r.move(i);
        MY_REQUIRE_EQUAL(i, val.first, "i = " << i);
        MY_REQUIRE_EQUAL(values[i], val.second, "i = " << i);
    }
}

TEST_CASE("positive_sequence")
{
    test_positive_sequence<pisa::strict_sequence>();
    test_positive_sequence<pisa::partitioned_sequence<pisa::strict_sequence>>();
    test_positive_sequence<pisa::uniform_partitioned_sequence<pisa::strict_sequence>>();
}
