#define CATCH_CONFIG_MAIN
#include "catch2/catch.hpp"

#include <algorithm>
#include <cstdlib>
#include <numeric>
#include <vector>

#include "test_generic_sequence.hpp"

#include "freq_index.hpp"
#include "mappable/mapper.hpp"
#include "memory_source.hpp"
#include "mio/mmap.hpp"
#include "sequence/indexed_sequence.hpp"
#include "sequence/partitioned_sequence.hpp"
#include "sequence/positive_sequence.hpp"
#include "sequence/uniform_partitioned_sequence.hpp"
#include "temporary_directory.hpp"

template <typename DocsSequence, typename FreqsSequence>
void test_freq_index()
{
    pisa::TemporaryDirectory tmpdir;
    auto idx_path = (tmpdir.path() / "coll.bin").string();

    pisa::global_parameters params;
    uint64_t universe = 20000;
    using collection_type = pisa::freq_index<DocsSequence, FreqsSequence>;
    typename collection_type::builder b(universe, params);

    using vec_type = std::vector<uint64_t>;
    std::vector<std::pair<vec_type, vec_type>> posting_lists(30);
    for (auto& plist: posting_lists) {
        double avg_gap = 1.1 + double(rand()) / RAND_MAX * 10;
        auto n = uint64_t(universe / avg_gap);
        plist.first = random_sequence(universe, n, true);
        plist.second.resize(n);
        std::generate(plist.second.begin(), plist.second.end(), []() { return (rand() % 256) + 1; });
        uint64_t freqs_sum = std::accumulate(plist.second.begin(), plist.second.end(), uint64_t(0));

        b.add_posting_list(n, plist.first.begin(), plist.second.begin(), freqs_sum);
    }

    {
        collection_type coll;
        b.build(coll);
        pisa::mapper::freeze(coll, idx_path.c_str());
    }

    {
        collection_type coll(pisa::MemorySource::mapped_file(idx_path));
        for (size_t i = 0; i < posting_lists.size(); ++i) {
            auto const& plist = posting_lists[i];
            auto doc_enum = coll[i];
            REQUIRE(plist.first.size() == doc_enum.size());
            for (size_t p = 0; p < plist.first.size(); ++p, doc_enum.next()) {
                MY_REQUIRE_EQUAL(plist.first[p], doc_enum.docid(), "i = " << i << " p = " << p);
                MY_REQUIRE_EQUAL(plist.second[p], doc_enum.freq(), "i = " << i << " p = " << p);
            }
            REQUIRE(coll.num_docs() == doc_enum.docid());
        }
    }
}

TEST_CASE("freq_index")
{
    using pisa::indexed_sequence;
    using pisa::partitioned_sequence;
    using pisa::positive_sequence;
    using pisa::strict_sequence;
    using pisa::uniform_partitioned_sequence;

    test_freq_index<indexed_sequence, positive_sequence<>>();

    test_freq_index<partitioned_sequence<>, positive_sequence<partitioned_sequence<strict_sequence>>>();
    test_freq_index<
        uniform_partitioned_sequence<>,
        positive_sequence<uniform_partitioned_sequence<strict_sequence>>>();
}
