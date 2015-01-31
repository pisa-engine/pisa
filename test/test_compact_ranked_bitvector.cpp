#define BOOST_TEST_MODULE compact_ranked_bitvector

#include "test_generic_sequence.hpp"

#include "compact_ranked_bitvector.hpp"
#include <vector>
#include <cstdlib>

struct sequence_initialization {
    sequence_initialization()
    {
        n = 100000;
        universe = n * 3;
        seq = random_sequence(universe, n, true);

        // high granularity to test more corner cases
        params.rb_log_rank1_sampling = 6;
        params.rb_log_sampling1 = 5;
        succinct::bit_vector_builder bvb;
        ds2i::compact_ranked_bitvector::write(bvb,
                                                        seq.begin(),
                                                        universe, seq.size(),
                                                        params);
        succinct::bit_vector(&bvb).swap(bv);
    }

    ds2i::global_parameters params;
    size_t n;
    size_t universe;
    uint64_t log_rank1_sampling;
    uint64_t log_sampling1;
    std::vector<uint64_t> seq;
    succinct::bit_vector bv;
};

BOOST_FIXTURE_TEST_CASE(compact_ranked_bitvector_construction,
                        sequence_initialization)
{

    // test pointers and rank samples
    ds2i::compact_ranked_bitvector::offsets of(0,
                                                         universe, seq.size(),
                                                         params);
    uint64_t rank = 0;
    for (uint64_t pos = 0; pos < of.universe; ++pos) {
        bool b = bv[of.bits_offset + pos];

        if (b) {
            MY_REQUIRE_EQUAL(seq[rank], pos, "rank = " << rank);
        }

        if (b && rank && (rank % (1 << of.log_sampling1)) == 0) {
            uint64_t ptr_offset = of.pointers1_offset +
                ((rank >> of.log_sampling1) - 1) * of.pointer_size;
            MY_REQUIRE_EQUAL(pos, bv.get_bits(ptr_offset, of.pointer_size),
                             "rank = " << rank);
        }

        if (pos && (pos % (1 << of.log_rank1_sampling) == 0)) {
            uint64_t sample_offset = of.rank1_samples_offset +
                ((pos >> of.log_rank1_sampling) - 1) * of.rank1_sample_size;
            MY_REQUIRE_EQUAL(rank, bv.get_bits(sample_offset, of.rank1_sample_size),
                             "pos = " << pos);
        }

        rank += b;
    }
}

BOOST_FIXTURE_TEST_CASE(compact_ranked_bitvector_singleton,
                        sequence_initialization)
{
    // test singleton sequences
    std::vector<uint64_t> short_seq;
    short_seq.push_back(0);
    test_sequence(ds2i::compact_ranked_bitvector(), params, 1, short_seq);
    short_seq[0] = 1;
    test_sequence(ds2i::compact_ranked_bitvector(), params, 2, short_seq);
}

BOOST_FIXTURE_TEST_CASE(compact_ranked_bitvector_enumerator,
                        sequence_initialization)
{
    ds2i::compact_ranked_bitvector::enumerator r(bv, 0,
                                                           universe, seq.size(),
                                                           params);
    test_sequence(r, seq);
}
