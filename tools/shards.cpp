#include <algorithm>
#include <thread>
#include <vector>

#include <CLI/CLI.hpp>
#include <gsl/span>
#include <spdlog/spdlog.h>
#include <tbb/task_group.h>
#include <tbb/global_control.h>

#include "app.hpp"
#include "binary_collection.hpp"
#include "compress.hpp"
#include "invert.hpp"
#include "reorder_docids.hpp"
#include "sharding.hpp"
#include "util/util.hpp"
#include "wand_data.hpp"

namespace invert = pisa::invert;
using pisa::CompressArgs;
using pisa::CreateWandDataArgs;
using pisa::format_shard;
using pisa::InvertArgs;
using pisa::ReorderDocuments;
using pisa::resolve_shards;
using pisa::Shard_Id;

int main(int argc, char** argv)
{
    spdlog::drop("");
    spdlog::set_default_logger(spdlog::stderr_color_mt(""));

    CLI::App app{"Executes commands for shards."};
    auto* invert =
        app.add_subcommand("invert", "Constructs an inverted index from a forward index.");
    auto* reorder = app.add_subcommand("reorder-docids", "Reorder document IDs.");
    auto* compress = app.add_subcommand("compress", "Compresses an inverted index");
    auto* wand = app.add_subcommand("wand-data", "Creates additional data for query processing.");
    InvertArgs invert_args(invert);
    ReorderDocuments reorder_args(reorder);
    CompressArgs compress_args(compress);
    CreateWandDataArgs wand_args(wand);
    app.require_subcommand(1);
    CLI11_PARSE(app, argc, argv);

    if (invert->parsed()) {
        tbb::global_control control(tbb::global_control::max_allowed_parallelism, invert_args.threads() + 1);
        spdlog::info("Number of worker threads: {}", invert_args.threads());
        Shard_Id shard_id{0};
        for (auto shard: resolve_shards(invert_args.input_basename())) {
            invert::invert_forward_index(
                format_shard(invert_args.input_basename(), shard_id),
                format_shard(invert_args.output_basename(), shard_id),
                invert_args.batch_size(),
                invert_args.threads());
            shard_id += 1;
        }
    }
    if (reorder->parsed()) {
        auto shards = resolve_shards(reorder_args.input_basename(), ".docs");
        spdlog::info("Processing {} shards", shards.size());
        for (auto shard: shards) {
            auto shard_args = reorder_args;
            shard_args.apply_shard(shard);
            if (auto ret = pisa::reorder_docids(shard_args); ret != 0) {
                return ret;
            }
        }
        return 0;
    }
    if (compress->parsed()) {
        auto shards = resolve_shards(compress_args.input_basename(), ".docs");
        spdlog::info("Processing {} shards", shards.size());
        for (auto shard: shards) {
            auto shard_args = compress_args;
            shard_args.apply_shard(shard);
            pisa::compress(
                shard_args.input_basename(),
                shard_args.wand_data_path(),
                shard_args.index_encoding(),
                shard_args.output(),
                shard_args.scorer_params(),
                shard_args.quantize(),
                shard_args.check());
        }
        return 0;
    }
    if (wand->parsed()) {
        auto shards = resolve_shards(wand_args.input_basename(), ".docs");
        spdlog::info("Processing {} shards", shards.size());
        for (auto shard: shards) {
            auto shard_args = wand_args;
            shard_args.apply_shard(shard);
            pisa::create_wand_data(
                shard_args.output(),
                shard_args.input_basename(),
                shard_args.block_size(),
                shard_args.scorer_params(),
                shard_args.range(),
                shard_args.compress(),
                shard_args.quantize(),
                shard_args.dropped_term_ids());
        }
    }
    return 0;
}
