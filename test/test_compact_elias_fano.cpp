#define BOOST_TEST_MODULE compact_elias_fano

#include "test_generic_sequence.hpp"

#include "compact_elias_fano.hpp"
#include <vector>
#include <cstdlib>

struct sequence_initialization {
    sequence_initialization()
    {
        n = 100000;
        universe = n * 1024;
        seq = random_sequence(universe, n);

        // high granularity to test more corner cases
        params.ef_log_sampling0 = 4;
        params.ef_log_sampling1 = 5;
        succinct::bit_vector_builder bvb;
        ds2i::compact_elias_fano::write(bvb,
                                                  seq.begin(),
                                                  universe, seq.size(),
                                                  params);
        succinct::bit_vector(&bvb).swap(bv);
    }

    ds2i::global_parameters params;
    size_t n;
    size_t universe;
    std::vector<uint64_t> seq;
    succinct::bit_vector bv;
};

BOOST_FIXTURE_TEST_CASE(compact_elias_fano_singleton,
                        sequence_initialization)
{
    // test singleton sequences
    std::vector<uint64_t> short_seq;
    short_seq.push_back(0);
    test_sequence(ds2i::compact_elias_fano(), params, 1, short_seq);
    short_seq[0] = 1;
    test_sequence(ds2i::compact_elias_fano(), params, 2, short_seq);
}

BOOST_FIXTURE_TEST_CASE(compact_elias_fano_construction,
                        sequence_initialization)
{

    // test pointers and low-level values
    ds2i::compact_elias_fano::offsets of(0,
                                                   universe, seq.size(),
                                                   params);
    uint64_t rank = 0;
    for (uint64_t pos = 0; pos < of.higher_bits_length; ++pos) {
        bool b = bv[of.higher_bits_offset + pos];
        uint64_t rank0 = pos - rank;

        if (b) {
            uint64_t read_v = ((pos - rank - 1) << of.lower_bits) |
                bv.get_bits(of.lower_bits_offset + rank * of.lower_bits,
                            of.lower_bits);
            MY_REQUIRE_EQUAL(seq[rank], read_v, "rank = " << rank);
        }

        if (b && rank && (rank % (1 << of.log_sampling1)) == 0) {
            uint64_t ptr_offset = of.pointers1_offset +
                ((rank >> of.log_sampling1) - 1) * of.pointer_size;
            MY_REQUIRE_EQUAL(pos, bv.get_bits(ptr_offset, of.pointer_size),
                             "rank = " << rank);
        }

        if (!b && rank0 && (rank0 % (1 << of.log_sampling0)) == 0) {
            uint64_t ptr_offset = of.pointers0_offset +
                ((rank0 >> of.log_sampling0) - 1) * of.pointer_size;
            MY_REQUIRE_EQUAL(pos, bv.get_bits(ptr_offset, of.pointer_size),
                             "rank0 = " << rank0);
        }
        rank += b;
    }
}

BOOST_FIXTURE_TEST_CASE(compact_elias_fano_enumerator,
                        sequence_initialization)
{
    ds2i::compact_elias_fano::enumerator r(bv, 0,
                                                     universe, seq.size(),
                                                     params);
    test_sequence(r, seq);
}

BOOST_FIXTURE_TEST_CASE(compact_elias_fano_weakly_monotone,
                        sequence_initialization)
{
    n = 100000;
    universe = n * 3;
    std::vector<uint64_t> seq = random_sequence(universe, n, false);
    test_sequence(ds2i::compact_elias_fano(), params, universe, seq);
}

