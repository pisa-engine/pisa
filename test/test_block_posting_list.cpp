#define BOOST_TEST_MODULE block_posting_list

#include "test_generic_sequence.hpp"

#include "block_posting_list.hpp"
#include "block_codecs.hpp"

#include <vector>
#include <cstdlib>
#include <algorithm>

template <typename PostingList>
void test_block_posting_list_ops(uint8_t const* data, uint64_t n, uint64_t universe,
                                 std::vector<uint64_t> const& docs,
                                 std::vector<uint64_t> const& freqs)
{
        typename PostingList::document_enumerator e(data, universe);
        BOOST_REQUIRE_EQUAL(n, e.size());
        for (size_t i = 0; i < n; ++i, e.next()) {
            MY_REQUIRE_EQUAL(docs[i], e.docid(),
                             "i = " << i << " size = " << n);
            MY_REQUIRE_EQUAL(freqs[i], e.freq(),
                             "i = " << i << " size = " << n);
        }
        // XXX better testing of next_geq
        for (size_t i = 0; i < n; ++i) {
            e.reset();
            e.next_geq(docs[i]);
            MY_REQUIRE_EQUAL(docs[i], e.docid(),
                             "i = " << i << " size = " << n);
            MY_REQUIRE_EQUAL(freqs[i], e.freq(),
                             "i = " << i << " size = " << n);
        }
        e.reset(); e.next_geq(docs.back() + 1);
        BOOST_REQUIRE_EQUAL(universe, e.docid());
        e.reset(); e.next_geq(universe);
        BOOST_REQUIRE_EQUAL(universe, e.docid());
}

void random_posting_data(uint64_t n, uint64_t universe,
                         std::vector<uint64_t>& docs,
                         std::vector<uint64_t>& freqs)
{
    docs = random_sequence(universe, n, true);
    freqs.resize(n);
    std::generate(freqs.begin(), freqs.end(),
                  []() { return (rand() % 256) + 1; });
}

template <typename BlockCodec>
void test_block_posting_list()
{
    typedef ds2i::block_posting_list<BlockCodec> posting_list_type;
    uint64_t universe = 20000;
    for (size_t t = 0; t < 20; ++t) {
        double avg_gap = 1.1 + double(rand()) / RAND_MAX * 10;
        uint64_t n = uint64_t(universe / avg_gap);

        std::vector<uint64_t> docs, freqs;
        random_posting_data(n, universe, docs, freqs);
        std::vector<uint8_t> data;
        posting_list_type::write(data, n, docs.begin(), freqs.begin());

        test_block_posting_list_ops<posting_list_type>(data.data(), n, universe,
                                                       docs, freqs);
    }
}

template <typename BlockCodec>
void test_block_posting_list_reordering()
{
    typedef ds2i::block_posting_list<BlockCodec> posting_list_type;
    uint64_t universe = 20000;
    for (size_t t = 0; t < 20; ++t) {
        double avg_gap = 1.1 + double(rand()) / RAND_MAX * 10;
        uint64_t n = uint64_t(universe / avg_gap);

        std::vector<uint64_t> docs, freqs;
        random_posting_data(n, universe, docs, freqs);
        std::vector<uint8_t> data;
        posting_list_type::write(data, n, docs.begin(), freqs.begin());

        // reorder blocks
        typename posting_list_type::document_enumerator e(data.data(), universe);
        auto blocks = e.get_blocks();
        std::random_shuffle(blocks.begin() + 1, blocks.end()); // leave first block in place

        std::vector<uint8_t> reordered_data;
        posting_list_type::write_blocks(reordered_data, n, blocks);

        test_block_posting_list_ops<posting_list_type>(reordered_data.data(), n, universe,
                                                       docs, freqs);
    }
}

BOOST_AUTO_TEST_CASE(block_posting_list)
{
    test_block_posting_list<ds2i::optpfor_block>();
    test_block_posting_list<ds2i::varint_G8IU_block>();
    test_block_posting_list<ds2i::interpolative_block>();
}

BOOST_AUTO_TEST_CASE(block_posting_list_reordering)
{
    test_block_posting_list_reordering<ds2i::optpfor_block>();
}
