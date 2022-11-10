#define CATCH_CONFIG_MAIN
#include "catch2/catch.hpp"

#include "mio/mmap.hpp"
#include "test_generic_sequence.hpp"

#include "mappable/mapper.hpp"
#include "sequence/indexed_sequence.hpp"
#include "sequence/partitioned_sequence.hpp"
#include "sequence/uniform_partitioned_sequence.hpp"
#include "sequence_collection.hpp"
#include "temporary_directory.hpp"

#include <cstdlib>
#include <vector>

template <typename BaseSequence>
void test_sequence_collection()
{
    pisa::global_parameters params;
    uint64_t universe = 10000;
    using collection_type = pisa::sequence_collection<BaseSequence>;
    typename collection_type::builder b(params);

    std::vector<std::vector<uint64_t>> sequences(30);
    for (auto& seq: sequences) {
        double avg_gap = 1.1 + double(rand()) / RAND_MAX * 10;
        auto n = uint64_t(universe / avg_gap);
        seq = random_sequence(universe, n, true);
        b.add_sequence(seq.begin(), seq.back() + 1, n);
    }

    pisa::TemporaryDirectory tmpdir;
    auto filename = tmpdir.path().string() + "temp.bin";
    {
        collection_type coll;
        b.build(coll);
        pisa::mapper::freeze(coll, filename.c_str());
    }

    {
        collection_type coll;
        mio::mmap_source m(filename.c_str());
        pisa::mapper::map(coll, m);

        for (size_t i = 0; i < sequences.size(); ++i) {
            test_sequence(coll[i], sequences[i]);
        }
    }
}

TEST_CASE("sequence_collection")
{
    test_sequence_collection<pisa::indexed_sequence>();
    test_sequence_collection<pisa::partitioned_sequence<>>();
    test_sequence_collection<pisa::uniform_partitioned_sequence<>>();
}
