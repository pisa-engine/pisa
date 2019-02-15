#define CATCH_CONFIG_MAIN
#include "catch2/catch.hpp"

#include <algorithm>
#include <cstdio>
#include <iostream>
#include <string>

#include <boost/filesystem.hpp>
#include <fmt/ostream.h>
#include <gsl/span>
#include <range/v3/view/drop.hpp>
#include <range/v3/view/iota.hpp>
#include <range/v3/view/stride.hpp>
#include <tbb/task_scheduler_init.h>

#include "binary_freq_collection.hpp"
#include "ds2i_config.hpp"
#include "filesystem.hpp"
#include "forward_index_builder.hpp"
#include "sharding.hpp"
#include "temporary_directory.hpp"

using namespace boost::filesystem;
using namespace pisa;
using namespace pisa::literals;

using posting_vector_type = std::vector<std::pair<Term_Id, Document_Id>>;
using iterator_type       = decltype(std::declval<posting_vector_type>().begin());
using index_type          = invert::Inverted_Index<iterator_type>;

[[nodiscard]] auto next_plaintext_record(std::istream &in) -> std::optional<pisa::Plaintext_Record>
{
    pisa::Plaintext_Record record;
    if (in >> record) {
        return record;
    }
    return std::nullopt;
}

TEST_CASE("create_random_mapping", "[invert][unit]")
{
    uint64_t seed = 88887;
    auto mapping = pisa::create_random_mapping(1000u, 13u, seed);
    Vector<Shard_Id, int> counts(13, 0);
    Vector<Document_Id> documents;
    for (auto &&[doc, shard] : mapping.entries()) {
        counts[shard] += 1;
        documents.push_back(doc);
    }
    std::sort(documents.begin(), documents.end());

    REQUIRE(documents.as_vector() ==
            ranges::to_vector(ranges::view::iota(Document_Id{}, Document_Id{1000u})));
    REQUIRE(counts.as_vector() ==
            std::vector<int>{77, 77, 77, 77, 77, 77, 77, 77, 77, 77, 77, 77, 76});
}

auto round_robin_mapping(int document_count, int shard_count)
{
    Vector<Document_Id, Shard_Id> mapping(document_count);
    Shard_Id shard = 0_s;
    for (auto doc : ranges::view::iota(0_d, Document_Id{document_count})) {
        mapping[doc] = shard++;
        if (shard == Shard_Id{shard_count}) {
            shard = 0_s;
        }
    }
    return mapping;
}

void build_fwd_index(std::string const &output)
{
    std::string input(DS2I_SOURCE_DIR "/test/test_data/clueweb1k.plaintext");
    std::ifstream is(input);
    pisa::Forward_Index_Builder<pisa::Plaintext_Record> builder;
    builder.build(is,
                  output,
                  next_plaintext_record,
                  [](std::string &&term) -> std::string { return std::forward<std::string>(term); },
                  pisa::parse_plaintext_content,
                  20'000,
                  2);
}

template <typename Container>
auto shard_elements(Container const &container, Shard_Id shard_id, int shard_count)
{
    Container elems;
    for (auto const &val :
         ranges::view::drop(container, shard_id.as_int()) | ranges::view::stride(shard_count))
    {
        elems.push_back(val);
    }
    return elems;
}

TEST_CASE("partition_fwd_index", "[invert][integration]")
{
    tbb::task_scheduler_init init(1);
    GIVEN("A test forward index")
    {
        Temporary_Directory dir;
        std::string fwd_basename = (dir.path() / "fwd").string();
        std::string output_basename = (dir.path() / "shards").string();
        int document_count = 10'000;
        build_fwd_index(fwd_basename);

        WHEN("Partition the forward index in a round-robin manner")
        {
            auto mapping = round_robin_mapping(document_count, 13);
            REQUIRE(mapping.size() == document_count);
            partition_fwd_index(fwd_basename, output_basename, mapping);
            auto shard_ids = ranges::view::iota(0_s, 13_s);

            THEN("Document titles are correctly partitioned")
            {
                auto original_titles =
                    io::read_string_vector(fmt::format("{}.documents", fwd_basename));
                for (auto shard_id : shard_ids) {
                    auto expected_titles = shard_elements(original_titles, shard_id, 13);
                    auto actual_titles = io::read_string_vector(
                        fmt::format("{}.{:03d}.documents", output_basename, shard_id.as_int()));
                    REQUIRE(actual_titles == expected_titles);
                }
            }
            AND_THEN("Documents are identical wrt terms")
            {
                auto full = binary_collection(fwd_basename.c_str());
                auto full_iter = ++full.begin();
                auto full_terms = io::read_string_vector(fmt::format("{}.terms", fwd_basename));
                std::vector<binary_collection> shards;
                std::vector<typename binary_collection::const_iterator> shard_iterators;
                std::vector<std::vector<std::string>> shard_terms;
                for (auto shard : shard_ids) {
                    shards.push_back(binary_collection(
                        fmt::format("{}.{:03d}", output_basename, shard.as_int()).c_str()));
                    shard_terms.push_back(io::read_string_vector(
                        fmt::format("{}.{:03d}.terms", output_basename, shard.as_int()).c_str()));
                    shard_iterators.push_back(++shards.back().begin());
                }
                Shard_Id shard = 0_s;
                for (auto doc : ranges::view::iota(0_d, Document_Id{document_count})) {
                    std::clog << "verifying document " << doc << '\n';
                    auto full_seq = *full_iter;
                    auto shard_seq = *shard_iterators[shard.as_int()];
                    std::vector<std::string> expected_documents(full_seq.size());
                    std::vector<std::string> actual_documents(shard_seq.size());
                    std::transform(full_seq.begin(),
                                   full_seq.end(),
                                   expected_documents.begin(),
                                   [&](auto const &id) { return full_terms[id]; });
                    std::transform(shard_seq.begin(),
                                   shard_seq.end(),
                                   actual_documents.begin(),
                                   [&](auto const &id) { return shard_terms[shard.as_int()][id]; });
                    shard += 1_s;
                    if (shard == 13_s) {
                        shard = 0_s;
                    }
                }
            }
        }
    }
}
