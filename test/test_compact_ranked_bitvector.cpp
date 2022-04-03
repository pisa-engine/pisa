#define CATCH_CONFIG_MAIN
#include "catch2/catch.hpp"

#include "test_generic_sequence.hpp"

#include "codec/compact_ranked_bitvector.hpp"

#include <cstdlib>
#include <vector>

struct sequence_initialization {
    sequence_initialization() : seq(random_sequence(universe, n, true))
    {
        // high granularity to test more corner cases
        params.rb_log_rank1_sampling = 6;
        params.rb_log_sampling1 = 5;
        pisa::bit_vector_builder bvb;
        pisa::compact_ranked_bitvector::write(bvb, seq.begin(), universe, seq.size(), params);
        pisa::bit_vector(&bvb).swap(bv);
    }

    pisa::global_parameters params;
    size_t n = 100000;
    size_t universe = 300000;
    std::vector<uint64_t> seq;
    pisa::bit_vector bv;
};

TEST_CASE_METHOD(sequence_initialization, "compact_ranked_bitvector_construction")
{
    // test pointers and rank samples
    pisa::compact_ranked_bitvector::offsets of(0, universe, seq.size(), params);
    uint64_t rank = 0;
    for (uint64_t pos = 0; pos < of.universe; ++pos) {
        bool b = bv[of.bits_offset + pos];

        if (b) {
            MY_REQUIRE_EQUAL(seq[rank], pos, "rank = " << rank);
        }

        if (b && (rank != 0U) && (rank % (1 << of.log_sampling1)) == 0) {
            uint64_t ptr_offset =
                of.pointers1_offset + ((rank >> of.log_sampling1) - 1) * of.pointer_size;
            MY_REQUIRE_EQUAL(pos, bv.get_bits(ptr_offset, of.pointer_size), "rank = " << rank);
        }

        if ((pos != 0U) && (pos % (1 << of.log_rank1_sampling) == 0)) {
            uint64_t sample_offset = of.rank1_samples_offset
                + ((pos >> of.log_rank1_sampling) - 1) * of.rank1_sample_size;
            MY_REQUIRE_EQUAL(rank, bv.get_bits(sample_offset, of.rank1_sample_size), "pos = " << pos);
        }

        rank += static_cast<unsigned long>(b);
    }
}

TEST_CASE_METHOD(sequence_initialization, "compact_ranked_bitvector_singleton")
{
    // test singleton sequences
    std::vector<uint64_t> short_seq;
    short_seq.push_back(0);
    test_sequence(pisa::compact_ranked_bitvector(), params, 1, short_seq);
    short_seq[0] = 1;
    test_sequence(pisa::compact_ranked_bitvector(), params, 2, short_seq);
}

TEST_CASE_METHOD(sequence_initialization, "compact_ranked_bitvector_enumerator")
{
    pisa::compact_ranked_bitvector::enumerator r(bv, 0, universe, seq.size(), params);
    test_sequence(r, seq);
}
