#define CATCH_CONFIG_MAIN
#include "catch2/catch.hpp"

#include "test_generic_sequence.hpp"

#include "codec/compact_elias_fano.hpp"
#include <cstdlib>
#include <vector>

struct sequence_initialization {
    sequence_initialization()
    {
        n = 100000;
        universe = n * 1024;
        seq = random_sequence(universe, n);

        // high granularity to test more corner cases
        params.ef_log_sampling0 = 4;
        params.ef_log_sampling1 = 5;
        pisa::bit_vector_builder bvb;
        pisa::compact_elias_fano::write(bvb, seq.begin(), universe, seq.size(), params);
        pisa::bit_vector(&bvb).swap(bv);
    }

    pisa::global_parameters params;
    size_t n;
    size_t universe;
    std::vector<uint64_t> seq;
    pisa::bit_vector bv;
};

TEST_CASE_METHOD(sequence_initialization, "compact_elias_fano_singleton")
{
    // test singleton sequences
    std::vector<uint64_t> short_seq;
    short_seq.push_back(0);
    test_sequence(pisa::compact_elias_fano(), params, 1, short_seq);
    short_seq[0] = 1;
    test_sequence(pisa::compact_elias_fano(), params, 2, short_seq);
}

TEST_CASE_METHOD(sequence_initialization, "compact_elias_fano_construction")
{
    // test pointers and low-level values
    pisa::compact_elias_fano::offsets of(0, universe, seq.size(), params);
    uint64_t rank = 0;
    for (uint64_t pos = 0; pos < of.higher_bits_length; ++pos) {
        bool b = bv[of.higher_bits_offset + pos];
        uint64_t rank0 = pos - rank;

        if (b) {
            uint64_t read_v = ((pos - rank - 1) << of.lower_bits)
                | bv.get_bits(of.lower_bits_offset + rank * of.lower_bits, of.lower_bits);
            MY_REQUIRE_EQUAL(seq[rank], read_v, "rank = " << rank);
        }

        if (b && (rank != 0U) && (rank % (1 << of.log_sampling1)) == 0) {
            uint64_t ptr_offset =
                of.pointers1_offset + ((rank >> of.log_sampling1) - 1) * of.pointer_size;
            MY_REQUIRE_EQUAL(pos, bv.get_bits(ptr_offset, of.pointer_size), "rank = " << rank);
        }

        if (!b && (rank0 != 0U) && (rank0 % (1 << of.log_sampling0)) == 0) {
            uint64_t ptr_offset =
                of.pointers0_offset + ((rank0 >> of.log_sampling0) - 1) * of.pointer_size;
            MY_REQUIRE_EQUAL(pos, bv.get_bits(ptr_offset, of.pointer_size), "rank0 = " << rank0);
        }
        rank += static_cast<uint64_t>(b);
    }
}

TEST_CASE_METHOD(sequence_initialization, "compact_elias_fano_enumerator")
{
    pisa::compact_elias_fano::enumerator r(bv, 0, universe, seq.size(), params);
    test_sequence(r, seq);
}

TEST_CASE_METHOD(sequence_initialization, "compact_elias_fano_weakly_monotone")
{
    n = 100000;
    universe = n * 3;
    std::vector<uint64_t> seq = random_sequence(universe, n, false);
    test_sequence(pisa::compact_elias_fano(), params, universe, seq);
}
