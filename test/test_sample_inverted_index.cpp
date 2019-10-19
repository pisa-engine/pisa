#define CATCH_CONFIG_MAIN
#include "catch2/catch.hpp"

#include "test_generic_sequence.hpp"
#include <algorithm>
#include <cstdlib>
#include <vector>

#include "binary_freq_collection.hpp"
#include "pisa_config.hpp"
#include "temporary_directory.hpp"
#include "util/inverted_index_utils.hpp"

using pisa::BinaryCollection;
using pisa::BinaryFreqCollection;

TEST_CASE("sample_inverted_index")
{
    // given
    std::string input(PISA_SOURCE_DIR "/test/test_data/test_collection");
    Temporary_Directory tmpdir;
    std::string output = tmpdir.path().string();
    auto original = BinaryFreqCollection(input.c_str());

    // when
    pisa::sample_inverted_index(input, output, [](size_t size) {
        std::vector<std::uint32_t> sample(size);
        std::iota(sample.begin(), sample.end(), 0);
        return sample;
    });
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

    BinaryCollection sizes_original((input + ".sizes").c_str());
    BinaryCollection sizes_sampled((output + ".sizes").c_str());
    auto soit = sizes_original.begin()->begin();
    auto ssit = sizes_sampled.begin()->begin();

    for (size_t i = 0; i < original.num_docs(); ++i) {
        REQUIRE(*soit++ == *ssit++);
    }
}

TEST_CASE("sample_inverted_index_one_sample")
{
    // given
    std::string input(PISA_SOURCE_DIR "/test/test_data/test_collection");
    Temporary_Directory tmpdir;
    std::string output = tmpdir.path().string();
    auto original = BinaryFreqCollection(input.c_str());

    // when
    pisa::sample_inverted_index(input, output, [](size_t size) {
        std::vector<std::uint32_t> sample(1);
        std::iota(sample.begin(), sample.end(), 0);
        return sample;
    });
    auto sampled = BinaryFreqCollection(output.c_str());

    // then
    REQUIRE(sampled.num_docs() == original.num_docs());
    auto oit = original.begin();
    auto sit = sampled.begin();
    for (auto i = 0; oit != original.end(); ++oit, ++sit) {
        std::vector<uint32_t> odocs(oit->documents.begin(), oit->documents.end());
        odocs.resize(1);
        std::vector<uint32_t> sdocs(sit->documents.begin(), sit->documents.end());
        std::vector<uint32_t> ofreqs(oit->frequencies.begin(), oit->frequencies.end());
        ofreqs.resize(1);
        std::vector<uint32_t> sfreqs(sit->frequencies.begin(), sit->frequencies.end());
        REQUIRE(std::equal(odocs.begin(), odocs.end(), sdocs.begin()));
        REQUIRE(std::equal(ofreqs.begin(), ofreqs.end(), sfreqs.begin()));
    }
    REQUIRE(sit == sampled.end());

    BinaryCollection sizes_original((input + ".sizes").c_str());
    BinaryCollection sizes_sampled((output + ".sizes").c_str());
    auto soit = sizes_original.begin()->begin();
    auto ssit = sizes_sampled.begin()->begin();

    for (size_t i = 0; i < original.num_docs(); ++i) {
        REQUIRE(*soit++ == *ssit++);
    }
}

TEST_CASE("sample_inverted_index_reverse")
{
    // given
    std::string input(PISA_SOURCE_DIR "/test/test_data/test_collection");
    Temporary_Directory tmpdir;
    std::string output = tmpdir.path().string();
    auto original = BinaryFreqCollection(input.c_str());
    float rate = 0.1;
    // when
    pisa::sample_inverted_index(input, output, [&](size_t size) {
        std::vector<std::uint32_t> sample(size);
        std::iota(sample.begin(), sample.end(), 0);
        std::reverse(sample.begin(), sample.end());
        size_t new_size = std::ceil(size * rate);
        sample.resize(new_size);
        std::sort(sample.begin(), sample.end());
        return sample;
    });
    auto sampled = BinaryFreqCollection(output.c_str());

    // then
    REQUIRE(sampled.num_docs() == original.num_docs());
    auto oit = original.begin();
    auto sit = sampled.begin();
    for (auto i = 0; oit != original.end(); ++oit, ++sit) {
        std::vector<uint32_t> odocs(oit->documents.begin(), oit->documents.end());
        std::reverse(odocs.begin(), odocs.end());
        size_t new_size = std::ceil(odocs.size() * rate);
        odocs.resize(new_size);
        std::reverse(odocs.begin(), odocs.end());
        std::vector<uint32_t> sdocs(sit->documents.begin(), sit->documents.end());
        std::vector<uint32_t> ofreqs(oit->frequencies.begin(), oit->frequencies.end());
        std::reverse(ofreqs.begin(), ofreqs.end());
        ofreqs.resize(new_size);
        std::reverse(ofreqs.begin(), ofreqs.end());
        std::vector<uint32_t> sfreqs(sit->frequencies.begin(), sit->frequencies.end());
        REQUIRE(std::equal(odocs.begin(), odocs.end(), sdocs.begin()));
        REQUIRE(std::equal(ofreqs.begin(), ofreqs.end(), sfreqs.begin()));
    }
    REQUIRE(sit == sampled.end());

    BinaryCollection sizes_original((input + ".sizes").c_str());
    BinaryCollection sizes_sampled((output + ".sizes").c_str());
    auto soit = sizes_original.begin()->begin();
    auto ssit = sizes_sampled.begin()->begin();

    for (size_t i = 0; i < original.num_docs(); ++i) {
        REQUIRE(*soit++ == *ssit++);
    }
}
