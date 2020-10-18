#define CATCH_CONFIG_MAIN
#include "catch2/catch.hpp"

#include "test_generic_sequence.hpp"

#include "codec/block_codecs.hpp"
#include "codec/maskedvbyte.hpp"
#include "codec/qmx.hpp"
#include "codec/simdbp.hpp"
#include "codec/simple16.hpp"
#include "codec/simple8b.hpp"
#include "codec/streamvbyte.hpp"
#include "codec/varintgb.hpp"
#include "temporary_directory.hpp"

#include "block_freq_index.hpp"
#include "mappable/mapper.hpp"
#include "mio/mmap.hpp"

#include <algorithm>
#include <cstdlib>
#include <vector>

template <typename BlockCodec>
void test_block_freq_index()
{
    pisa::global_parameters params;
    uint64_t universe = 20000;
    using collection_type = pisa::block_freq_index<BlockCodec>;
    typename collection_type::builder b(universe, params);

    using vec_type = std::vector<uint64_t>;
    std::vector<std::pair<vec_type, vec_type>> posting_lists(30);
    for (auto& plist: posting_lists) {
        double avg_gap = 1.1 + double(rand()) / RAND_MAX * 10;
        auto n = uint64_t(universe / avg_gap);
        plist.first = random_sequence(universe, n, true);
        plist.second.resize(n);
        std::generate(plist.second.begin(), plist.second.end(), []() { return (rand() % 256) + 1; });

        b.add_posting_list(n, plist.first.begin(), plist.second.begin(), 0);
    }

    pisa::TemporaryDirectory tmpdir;
    auto filename = (tmpdir.path() / "temp.bin").string();
    {
        collection_type coll;
        b.build(coll);
        pisa::mapper::freeze(coll, filename.c_str());
    }

    {
        collection_type coll(pisa::MemorySource::mapped_file(filename));
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

TEST_CASE("block_freq_index")
{
    test_block_freq_index<pisa::optpfor_block>();
    test_block_freq_index<pisa::varint_G8IU_block>();
    test_block_freq_index<pisa::streamvbyte_block>();
    test_block_freq_index<pisa::maskedvbyte_block>();
    test_block_freq_index<pisa::varintgb_block>();
    test_block_freq_index<pisa::interpolative_block>();
    test_block_freq_index<pisa::qmx_block>();
    test_block_freq_index<pisa::simple8b_block>();
    test_block_freq_index<pisa::simple16_block>();
    test_block_freq_index<pisa::simdbp_block>();
}
