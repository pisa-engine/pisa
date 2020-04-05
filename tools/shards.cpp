#include <algorithm>
#include <thread>
#include <vector>

#include <CLI/CLI.hpp>
#include <gsl/span>
#include <spdlog/spdlog.h>
#include <tbb/task_group.h>
#include <tbb/task_scheduler_init.h>

#include "app.hpp"
#include "binary_collection.hpp"
#include "compress.hpp"
#include "invert.hpp"
#include "recursive_graph_bisection.hpp"
#include "sharding.hpp"
#include "util/util.hpp"

namespace invert = pisa::invert;
using pisa::CompressArgs;
using pisa::format_shard;
using pisa::InvertArgs;
using pisa::RecursiveGraphBisectionArgs;
using pisa::resolve_shard_basenames;
using pisa::Shard_Id;

int main(int argc, char** argv)
{
    spdlog::drop("");
    spdlog::set_default_logger(spdlog::stderr_color_mt(""));

    CLI::App app{"Executes commands for shards."};
    auto* invert =
        app.add_subcommand("invert", "Constructs an inverted index from a forward index.");
    auto* bp = app.add_subcommand(
        "recursive-graph-bisection",
        "Recursive graph bisection algorithm used for inverted indexed reordering.");
    auto* compress = app.add_subcommand("compress", "Compresses an inverted index");
    InvertArgs invert_args(invert);
    RecursiveGraphBisectionArgs bp_args(&app);
    CompressArgs args(&app);
    app.require_subcommand(1);
    CLI11_PARSE(app, argc, argv);

    if (invert->parsed()) {
        tbb::task_scheduler_init init(invert_args.threads());
        spdlog::info("Number of threads: {}", invert_args.threads());
        Shard_Id shard_id{0};
        for (auto shard: resolve_shard_basenames(invert_args.input_basename())) {
            invert::invert_forward_index(
                shard.string(),
                format_shard(invert_args.output_basename(), shard_id),
                invert_args.batch_size(),
                invert_args.threads());
            shard_id += 1;
        }
    }
    if (bp->parsed()) {
        if (not bp_args.output_basename() && not bp_args.output_fwd()) {
            spdlog::error("Must define at least one output parameter.");
            return 1;
        }
        tbb::task_scheduler_init init(bp_args.threads());
        spdlog::info("Number of threads: {}", bp_args.threads());
        pisa::bp::run(bp_args);
    }
    if (compress->parsed()) {
        pisa::compress_index(args);
    }
    return 0;
}
