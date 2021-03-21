#include "wand_utils.hpp"
#define CATCH_CONFIG_MAIN

#include <catch2/catch.hpp>
#include <functional>

#include <fmt/format.h>

#include "binary_freq_collection.hpp"
#include "common_sharding.hpp"
#include "compress.hpp"
#include "cursor/block_max_scored_cursor.hpp"
#include "cursor/max_scored_cursor.hpp"
#include "cursor/scored_cursor.hpp"
#include "filesystem.hpp"
#include "forward_index_builder.hpp"
#include "index_types.hpp"
#include "invert.hpp"
#include "parsing/html.hpp"
#include "payload_vector.hpp"
#include "pisa_config.hpp"
#include "query/algorithm/maxscore_query.hpp"
#include "query/algorithm/ranked_or_query.hpp"
#include "query/queries.hpp"
#include "scorer/scorer.hpp"
#include "sharding.hpp"
#include "temporary_directory.hpp"
#include "test_common.hpp"
#include "wand_data.hpp"

using namespace pisa;

auto queries(std::string const& term_lexicon)
{
    constexpr auto input =
        "hello world\n"
        "index\n"
        "web search\n"
        "calendar\n"
        "kitty\n"
        "terminal emulator\n"
        "0000\n"
        "monthly basis\n"
        "email client linux\n"
        "tea drunk in the future";
    std::vector<Query> queries;
    auto parse_query = resolve_query_parser(queries, term_lexicon, {}, "porter2");
    std::istringstream is(input);
    io::for_each_line(is, parse_query);
    return queries;
}

TEST_CASE("partition_fwd_index", "[invert][integration]")
{
    GIVEN("A test forward index")
    {
        Temporary_Directory dir;
        std::string fwd_basename = (dir.path() / "fwd").string();
        std::string output_basename = (dir.path() / "shards").string();
        int document_count = 1'000;
        build_fwd_index(fwd_basename);
        ScorerParams scorer("bm25");

        WHEN("Partition the forward index in a round-robin manner")
        {
            auto mapping = round_robin_mapping(document_count, 13);
            REQUIRE(mapping.size() == document_count);
            partition_fwd_index(fwd_basename, output_basename, mapping);
            auto shard_ids = ranges::views::iota(0_s, 13_s);

            std::string inv_basename = (dir.path() / "inv").string();
            std::string global_index_path = (dir.path() / "simdbp").string();
            std::string global_wdata_path = (dir.path() / "wdata").string();
            std::string global_termlex_path = (dir.path() / "fwd.termlex").string();
            std::string global_doclex_path = (dir.path() / "fwd.doclex").string();
            std::vector<std::string> local_index_paths;
            std::vector<std::string> local_wdata_paths;
            pisa::invert::invert_forward_index(fwd_basename, inv_basename, 10000, 1);
            pisa::compress(
                inv_basename, {}, "block_optpfor", global_index_path, scorer, false, false);
            pisa::create_wand_data(
                global_wdata_path,
                inv_basename,
                BlockSize(FixedBlock(128)),
                scorer,
                false,
                false,
                false,
                {});
            for (auto shard_id: shard_ids) {
                std::string fwd = (dir.path() / format_shard("shards", shard_id)).string();
                std::string inv = (dir.path() / format_shard("inv", shard_id)).string();
                std::string local = (dir.path() / format_shard("simdbp", shard_id)).string();
                std::string wand = (dir.path() / format_shard("wdata", shard_id)).string();
                std::string termlex =
                    (dir.path() / format_shard("shards", shard_id, ".termlex")).string();
                std::string doclex =
                    (dir.path() / format_shard("shards", shard_id, ".doclex")).string();
                pisa::invert::invert_forward_index(fwd, inv, 10000, 1);
                pisa::compress(inv, {}, "block_optpfor", local, scorer, false, true);
                pisa::create_wand_data(
                    wand,
                    inv,
                    BlockSize(FixedBlock(128)),
                    scorer,
                    false,
                    false,
                    false,
                    {},
                    GlobalDataPaths{
                        global_wdata_path, global_termlex_path, termlex, global_doclex_path, doclex});
                local_index_paths.push_back(std::move(local));
                local_wdata_paths.push_back(std::move(wand));
            }

            THEN("Querying shards returns the same results")
            {
                topk_queue local_heap(10);
                maxscore_query local(local_heap);

                topk_queue global_heap(10);
                maxscore_query global(global_heap);
                block_optpfor_index const global_index(MemorySource::mapped_file(global_index_path));
                wand_data<wand_data_raw> const global_wdata(
                    MemorySource::mapped_file(global_wdata_path));
                auto global_scorer = scorer::from_params(ScorerParams("bm25"), global_wdata);

                std::vector<std::vector<float>> global_results;

                for (auto const& q: queries((dir.path() / "fwd.termlex").string())) {
                    global_results.push_back({});
                    global(
                        make_max_scored_cursors(global_index, global_wdata, *global_scorer, q),
                        global_index.num_docs());
                    global_heap.finalize();
                    std::transform(
                        global_heap.topk().begin(),
                        global_heap.topk().end(),
                        std::back_inserter(global_results.back()),
                        [](auto p) { return p.first; });
                    global_heap.clear();
                }

                std::vector<std::vector<float>> local_results(global_results.size());

                for (auto shard_id: shard_ids) {
                    block_optpfor_index const local_index(
                        MemorySource::mapped_file(local_index_paths[shard_id.as_int()]));
                    wand_data<wand_data_raw> const local_wdata(
                        MemorySource::mapped_file(local_wdata_paths[shard_id.as_int()]));

                    REQUIRE(local_wdata.avg_len() == global_wdata.avg_len());

                    std::size_t qid = 0;
                    for (auto const& q: queries(
                             (dir.path() / format_shard("shards", shard_id, ".termlex")).string())) {
                        auto local_scorer = scorer::from_params(ScorerParams("bm25"), local_wdata);
                        local(
                            make_max_scored_cursors(local_index, local_wdata, *local_scorer, q),
                            local_index.num_docs());
                        local_heap.finalize();
                        std::vector<float> scores;
                        std::transform(
                            local_heap.topk().begin(),
                            local_heap.topk().end(),
                            std::back_inserter(local_results[qid]),
                            [](auto p) { return p.first; });
                        local_heap.clear();
                        ++qid;
                    }
                }
                for (std::size_t qid = 0; qid < global_results.size(); ++qid) {
                    std::sort(local_results[qid].begin(), local_results[qid].end(), std::greater<>{});
                    CAPTURE(qid);
                    CAPTURE(local_results[qid]);
                    std::vector<float> topk(
                        local_results[qid].begin(),
                        std::next(local_results[qid].begin(), global_results[qid].size()));
                    REQUIRE(topk == global_results[qid]);
                }
            }
        }
    }
}
