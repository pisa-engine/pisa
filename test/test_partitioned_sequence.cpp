#define BOOST_TEST_MODULE partitioned_sequence

#include <vector>
#include <cstdlib>
#include <iostream>

#include "test_generic_sequence.hpp"
#include "partitioned_sequence.hpp"
#include "strict_sequence.hpp"

namespace ds2i {

    class partitioned_sequence_test {
    public:
        template <typename Enumerator>
        static void test_construction(Enumerator& r, std::vector<uint64_t> const& seq)
        {
            if (r.m_partitions == 1) { // nothing to test here
                return;
            }

            for (size_t p = 0; p < r.m_partitions; ++p) {
                r.switch_partition(p);

                uint64_t cur_begin = r.m_cur_begin;
                uint64_t cur_end = r.m_cur_end;

                uint64_t cur_base = p ? seq[cur_begin - 1] + 1 : seq[0];
                uint64_t cur_upper_bound = seq[cur_end - 1];
                MY_REQUIRE_EQUAL(cur_base, r.m_cur_base,
                                 "p = " << p);
                MY_REQUIRE_EQUAL(cur_upper_bound, r.m_cur_upper_bound,
                                 "p = " << p);

                for (uint64_t i = cur_begin; i < cur_end; ++i) {
                    auto val = r.m_partition_enum.move(i - cur_begin);
                    MY_REQUIRE_EQUAL(seq[i], cur_base + val.second,
                                     "p = " << p << " i = " << i);
                }
            }
        }
    };
}

template <typename BaseSequence>
void test_partitioned_sequence(uint64_t universe,
                               std::vector<uint64_t> const& seq)
{
    ds2i::global_parameters params;
    typedef ds2i::partitioned_sequence<BaseSequence> sequence_type;

    succinct::bit_vector_builder bvb;
    sequence_type::write(bvb, seq.begin(), universe, seq.size(), params);
    succinct::bit_vector bv(&bvb);

    typename sequence_type::enumerator r(bv, 0, universe, seq.size(), params);
    ds2i::partitioned_sequence_test::test_construction(r, seq);
    test_sequence(r, seq);
}

BOOST_AUTO_TEST_CASE(partitioned_sequence)
{
    using ds2i::indexed_sequence;
    using ds2i::strict_sequence;

    if (boost::unit_test::framework::master_test_suite().argc == 2) {
        const char* filename = boost::unit_test::framework::master_test_suite().argv[1];
        std::cerr << "Testing sequence from file " << filename << std::endl;
        std::ifstream is(filename);
        uint64_t v;
        std::vector<uint64_t> seq;
        while (is >> v) {
            seq.push_back(v);
        }
        uint64_t universe = seq.back() + 1;
        test_partitioned_sequence<indexed_sequence>(universe, seq);
        test_partitioned_sequence<strict_sequence>(universe, seq);
        return;
    }

    // test singleton sequences
    {
        std::vector<uint64_t> seq;
        seq.push_back(0);
        test_partitioned_sequence<indexed_sequence>(1, seq);
        test_partitioned_sequence<strict_sequence>(1, seq);
        seq[0] = 1;
        test_partitioned_sequence<indexed_sequence>(2, seq);
        test_partitioned_sequence<strict_sequence>(2, seq);
    }

    std::vector<double> avg_gaps = { 1.1, 1.9, 2.5, 3, 4, 5, 10 };
    for (auto avg_gap: avg_gaps) {
        uint64_t n = 10000;
        uint64_t universe = uint64_t(n * avg_gap);
        auto seq = random_sequence(universe, n, true);
        test_partitioned_sequence<indexed_sequence>(universe, seq);
        test_partitioned_sequence<strict_sequence>(universe, seq);
    }

    // test also short (singleton partition) sequences with large universe
    for (size_t i = 1; i < 512; i += 41) {
        uint64_t universe = 100000;
        uint64_t initial_gap = rand() % 50000;
        auto short_seq = random_sequence(universe - initial_gap, i, true);
        for (auto& v: short_seq) v += initial_gap;
        test_partitioned_sequence<indexed_sequence>(universe, short_seq);
        test_partitioned_sequence<strict_sequence>(universe, short_seq);
    }

}
