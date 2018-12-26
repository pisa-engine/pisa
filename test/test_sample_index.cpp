#define BOOST_TEST_MODULE sample_index

#include "test_generic_sequence.hpp"

#include "codec/block_codecs.hpp"
#include "codec/maskedvbyte.hpp"
#include "codec/streamvbyte.hpp"
#include "codec/qmx.hpp"
#include "codec/varintgb.hpp"
#include "codec/simple8b.hpp"
#include "codec/simple16.hpp"
#include "codec/simdbp.hpp"

#include "block_posting_list.hpp"

#include <vector>
#include <cstdlib>
#include <algorithm>
#include "test_generic_sequence.hpp"

#include "binary_freq_collection.hpp"
#include "util/index_build_utils.hpp"

#include <vector>
#include <cstdlib>
#include <algorithm>
#include <numeric>

BOOST_AUTO_TEST_CASE(sample_index_full)
{
    // given
    using ds2i::binary_freq_collection;
    std::string input("test_data/test_collection");
    std::string output("temp_collection");
    auto original = binary_freq_collection(input.c_str());

    // when
    ds2i::sample_inverted_index(input, output, 10000);
    auto sampled = binary_freq_collection(output.c_str());

    // then
    BOOST_REQUIRE_EQUAL(sampled.num_docs(), original.num_docs());
    auto oit = original.begin();
    auto sit = sampled.begin();
    for (; oit != original.end(); ++oit, ++sit) {
        std::vector<uint32_t> odocs(oit->docs.begin(), oit->docs.end());
        std::vector<uint32_t> sdocs(sit->docs.begin(), sit->docs.end());
        std::vector<uint32_t> ofreqs(oit->freqs.begin(), oit->freqs.end());
        std::vector<uint32_t> sfreqs(sit->freqs.begin(), sit->freqs.end());
        BOOST_CHECK_EQUAL_COLLECTIONS(odocs.begin(), odocs.end(), sdocs.begin(), sdocs.end());
        BOOST_CHECK_EQUAL_COLLECTIONS(ofreqs.begin(), ofreqs.end(), sfreqs.begin(), sfreqs.end());
    }
    BOOST_REQUIRE_EQUAL(sit == sampled.end(), true);
}

BOOST_AUTO_TEST_CASE(sample_index)
{
    // given
    using ds2i::binary_freq_collection;
    std::string input("test_data/test_collection");
    std::string output("temp_collection");
    auto original = binary_freq_collection(input.c_str());
    size_t doc_limit = 2000;

    // when
    ds2i::sample_inverted_index(input, output, doc_limit);
    auto sampled = binary_freq_collection(output.c_str());

    // then
    BOOST_REQUIRE_EQUAL(sampled.num_docs(), doc_limit);
    auto oit = original.begin();
    auto sit = sampled.begin();
    for (; oit != original.end(); ++oit, ++sit) {
        std::vector<uint32_t> sdocs(sit->docs.begin(), sit->docs.end());
        std::vector<uint32_t> sfreqs(sit->freqs.begin(), sit->freqs.end());

        BOOST_REQUIRE_GT(sdocs.size(), 0);
        BOOST_REQUIRE_GT(sfreqs.size(), 0);
        BOOST_REQUIRE_EQUAL(
            std::count_if(sdocs.begin(), sdocs.end(), [&](const auto &d) { return d >= doc_limit; }),
            0);
    }
}
