#define CATCH_CONFIG_MAIN
#include "catch2/catch.hpp"

#include "test_generic_sequence.hpp"
#include <algorithm>
#include <cstdlib>
#include <unordered_set>
#include <vector>

#include "binary_freq_collection.hpp"
#include "pisa_config.hpp"
#include "temporary_directory.hpp"
#include "util/inverted_index_utils.hpp"

TEST_CASE("sample_inverted_index")
{
    // given
    using pisa::binary_freq_collection;
    std::string input(PISA_SOURCE_DIR "/test/test_data/test_collection");
    pisa::TemporaryDirectory tmpdir;
    std::string output = tmpdir.path().string();
    auto original = binary_freq_collection(input.c_str());

    // when
    std::unordered_set<size_t> terms_to_drop;
    pisa::sample_inverted_index(
        input,
        output,
        [](const auto& docs) {
            std::vector<std::uint32_t> sample(docs.size());
            std::iota(sample.begin(), sample.end(), 0);
            return sample;
        },
        terms_to_drop);
    auto sampled = binary_freq_collection(output.c_str());

    // then
    REQUIRE(sampled.num_docs() == original.num_docs());
    auto oit = original.begin();
    auto sit = sampled.begin();
    for (; oit != original.end(); ++oit, ++sit) {
        std::vector<uint32_t> odocs(oit->docs.begin(), oit->docs.end());
        std::vector<uint32_t> sdocs(sit->docs.begin(), sit->docs.end());
        std::vector<uint32_t> ofreqs(oit->freqs.begin(), oit->freqs.end());
        std::vector<uint32_t> sfreqs(sit->freqs.begin(), sit->freqs.end());
        REQUIRE(std::equal(odocs.begin(), odocs.end(), sdocs.begin()));
        REQUIRE(std::equal(ofreqs.begin(), ofreqs.end(), sfreqs.begin()));
    }
    REQUIRE(sit == sampled.end());

    pisa::binary_collection sizes_original((input + ".sizes").c_str());
    pisa::binary_collection sizes_sampled((output + ".sizes").c_str());
    auto soit = sizes_original.begin()->begin();
    auto ssit = sizes_sampled.begin()->begin();

    for (size_t i = 0; i < original.num_docs(); ++i) {
        REQUIRE(*soit++ == *ssit++);
    }
}

TEST_CASE("sample_inverted_index_one_sample")
{
    // given
    using pisa::binary_freq_collection;
    std::string input(PISA_SOURCE_DIR "/test/test_data/test_collection");
    pisa::TemporaryDirectory tmpdir;
    std::string output = tmpdir.path().string();
    auto original = binary_freq_collection(input.c_str());

    // when
    std::unordered_set<size_t> terms_to_drop;
    pisa::sample_inverted_index(
        input,
        output,
        [](const auto& docs) {
            std::vector<std::uint32_t> sample(1);
            std::iota(sample.begin(), sample.end(), 0);
            return sample;
        },
        terms_to_drop);
    auto sampled = binary_freq_collection(output.c_str());

    // then
    REQUIRE(sampled.num_docs() == original.num_docs());
    auto oit = original.begin();
    auto sit = sampled.begin();
    for (auto i = 0; oit != original.end(); ++oit, ++sit) {
        std::vector<uint32_t> odocs(oit->docs.begin(), oit->docs.end());
        odocs.resize(1);
        std::vector<uint32_t> sdocs(sit->docs.begin(), sit->docs.end());
        std::vector<uint32_t> ofreqs(oit->freqs.begin(), oit->freqs.end());
        ofreqs.resize(1);
        std::vector<uint32_t> sfreqs(sit->freqs.begin(), sit->freqs.end());
        REQUIRE(std::equal(odocs.begin(), odocs.end(), sdocs.begin()));
        REQUIRE(std::equal(ofreqs.begin(), ofreqs.end(), sfreqs.begin()));
    }
    REQUIRE(sit == sampled.end());

    pisa::binary_collection sizes_original((input + ".sizes").c_str());
    pisa::binary_collection sizes_sampled((output + ".sizes").c_str());
    auto soit = sizes_original.begin()->begin();
    auto ssit = sizes_sampled.begin()->begin();

    for (size_t i = 0; i < original.num_docs(); ++i) {
        REQUIRE(*soit++ == *ssit++);
    }
}

TEST_CASE("sample_inverted_index_reverse")
{
    // given
    using pisa::binary_freq_collection;
    std::string input(PISA_SOURCE_DIR "/test/test_data/test_collection");
    pisa::TemporaryDirectory tmpdir;
    std::string output = tmpdir.path().string();
    auto original = binary_freq_collection(input.c_str());
    float rate = 0.1;
    // when
    std::unordered_set<size_t> terms_to_drop;
    pisa::sample_inverted_index(
        input,
        output,
        [&](const auto& docs) {
            std::vector<std::uint32_t> sample(docs.size());
            std::iota(sample.begin(), sample.end(), 0);
            std::reverse(sample.begin(), sample.end());
            size_t new_size = std::ceil(docs.size() * rate);
            sample.resize(new_size);
            std::sort(sample.begin(), sample.end());
            return sample;
        },
        terms_to_drop);
    auto sampled = binary_freq_collection(output.c_str());

    // then
    REQUIRE(sampled.num_docs() == original.num_docs());
    auto oit = original.begin();
    auto sit = sampled.begin();
    for (auto i = 0; oit != original.end(); ++oit, ++sit) {
        std::vector<uint32_t> odocs(oit->docs.begin(), oit->docs.end());
        std::reverse(odocs.begin(), odocs.end());
        size_t new_size = std::ceil(odocs.size() * rate);
        odocs.resize(new_size);
        std::reverse(odocs.begin(), odocs.end());
        std::vector<uint32_t> sdocs(sit->docs.begin(), sit->docs.end());
        std::vector<uint32_t> ofreqs(oit->freqs.begin(), oit->freqs.end());
        std::reverse(ofreqs.begin(), ofreqs.end());
        ofreqs.resize(new_size);
        std::reverse(ofreqs.begin(), ofreqs.end());
        std::vector<uint32_t> sfreqs(sit->freqs.begin(), sit->freqs.end());
        REQUIRE(std::equal(odocs.begin(), odocs.end(), sdocs.begin()));
        REQUIRE(std::equal(ofreqs.begin(), ofreqs.end(), sfreqs.begin()));
    }
    REQUIRE(sit == sampled.end());

    pisa::binary_collection sizes_original((input + ".sizes").c_str());
    pisa::binary_collection sizes_sampled((output + ".sizes").c_str());
    auto soit = sizes_original.begin()->begin();
    auto ssit = sizes_sampled.begin()->begin();

    for (size_t i = 0; i < original.num_docs(); ++i) {
        REQUIRE(*soit++ == *ssit++);
    }
}
