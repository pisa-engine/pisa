#define CATCH_CONFIG_MAIN
#include "catch2/catch.hpp"

#include "test_generic_sequence.hpp"

#include "sequence/positive_sequence.hpp"
#include "sequence/partitioned_sequence.hpp"
#include "sequence/uniform_partitioned_sequence.hpp"
#include <vector>
#include <cstdlib>
#include <algorithm>
#include <numeric>

template <typename BaseSequence>
void test_positive_sequence()
{
    srand(42);
    ds2i::global_parameters params;
    size_t n = 50000;
    std::vector<uint64_t> values(n);
    std::generate(values.begin(), values.end(), []() { return (rand() % 256) + 1; });
    uint64_t universe = std::accumulate(values.begin(), values.end(), 0) + 1;

    typedef ds2i::positive_sequence<BaseSequence> sequence_type;
    ds2i::bit_vector_builder bvb;
    sequence_type::write(bvb, values.begin(), universe, values.size(), params);
    ds2i::bit_vector bv(&bvb);
    typename sequence_type::enumerator r(bv, 0, universe, values.size(), params);

    for (size_t i = 0; i < n; ++i) {
        auto val = r.move(i);
        MY_REQUIRE_EQUAL(i, val.first,
                         "i = " << i);
        MY_REQUIRE_EQUAL(values[i], val.second,
                         "i = " << i);
    }
}

TEST_CASE("positive_sequence")
{
    test_positive_sequence<ds2i::strict_sequence>();
    test_positive_sequence<ds2i::partitioned_sequence<ds2i::strict_sequence>>();
    test_positive_sequence<ds2i::uniform_partitioned_sequence<ds2i::strict_sequence>>();
}
