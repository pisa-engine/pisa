#define CATCH_CONFIG_MAIN
#include "catch2/catch.hpp"

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
#include "temporary_directory.hpp"

#include <vector>
#include <cstdlib>
#include <algorithm>
#include <numeric>

TEST_CASE( "sample_index_full")
{
    // given
    using pisa::BinaryFreqCollection;
    std::string input("test_data/test_collection");
    Temporary_Directory tmpdir;
    std::string output = tmpdir.path().string();
    auto original = BinaryFreqCollection(input.c_str());

    // when
    pisa::sample_inverted_index(input, output, 10000);
    auto sampled = BinaryFreqCollection(output.c_str());

    // then
    REQUIRE(sampled.num_docs() == original.num_docs());
    auto oit = original.begin();
    auto sit = sampled.begin();
    for (; oit != original.end(); ++oit, ++sit) {
        std::vector<uint32_t> odocs(oit->documents.begin(), oit->documents.end());
        std::vector<uint32_t> sdocs(sit->documents.begin(), sit->documents.end());
        std::vector<uint32_t> ofreqs(oit->frequencies.begin(), oit->frequencies.end());
        std::vector<uint32_t> sfreqs(sit->frequencies.begin(), sit->frequencies.end());
        REQUIRE(std::equal(odocs.begin(), odocs.end(), sdocs.begin()));
        REQUIRE(std::equal(ofreqs.begin(), ofreqs.end(), sfreqs.begin()));
    }
    REQUIRE(sit == sampled.end());
}

TEST_CASE( "sample_index")
{
    // given
    using pisa::BinaryFreqCollection;
    std::string input("test_data/test_collection");
    Temporary_Directory tmpdir;
    std::string output = tmpdir.path().string();
    auto original = BinaryFreqCollection(input.c_str());
    size_t doc_limit = 2000;

    // when
    pisa::sample_inverted_index(input, output, doc_limit);
    auto sampled = BinaryFreqCollection(output.c_str());

    // then
    REQUIRE(sampled.num_docs() == doc_limit);
    auto oit = original.begin();
    auto sit = sampled.begin();
    for (; oit != original.end(); ++oit, ++sit) {
        std::vector<uint32_t> sdocs(sit->documents.begin(), sit->documents.end());
        std::vector<uint32_t> sfreqs(sit->frequencies.begin(), sit->frequencies.end());

        REQUIRE(sdocs.size() > 0);
        REQUIRE(sfreqs.size() > 0);
        REQUIRE(
            std::count_if(sdocs.begin(), sdocs.end(), [&](const auto &d) { return d >= doc_limit; }) == 0);
    }
}
