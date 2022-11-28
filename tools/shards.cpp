#include <algorithm>
#include <thread>
#include <vector>

#include <CLI/CLI.hpp>
#include <gsl/span>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/spdlog.h>
#include <tbb/global_control.h>
#include <tbb/task_group.h>

#include "./taily_stats.hpp"
#include "./taily_thresholds.hpp"
#include "app.hpp"
#include "binary_collection.hpp"
#include "compress.hpp"
#include "invert.hpp"
#include "reorder_docids.hpp"
#include "sharding.hpp"
#include "util/util.hpp"
#include "vec_map.hpp"
#include "wand_data.hpp"

namespace invert = pisa::invert;
using pisa::CompressArgs;
using pisa::CreateWandDataArgs;
using pisa::format_shard;
using pisa::InvertArgs;
using pisa::ReorderDocuments;
using pisa::resolve_shards;
using pisa::Shard_Id;
using pisa::TailyRankArgs;
using pisa::TailyStatsArgs;
using pisa::TailyThresholds;
using pisa::invert::InvertParams;

void print_taily_scores(std::vector<double> const& scores, std::chrono::microseconds time)
{
    std::cout << R"({"time":)" << time.count() << R"(,"scores":[)";
    if (!scores.empty()) {
        std::cout << scores.front();
        std::for_each(std::next(scores.begin()), scores.end(), [](double score) {
            std::cout << ',' << score;
        });
    }
    std::cout << "]}\n";
}

int main(int argc, char** argv)
{
    spdlog::drop("");
    spdlog::set_default_logger(spdlog::stderr_color_mt(""));

    pisa::App<pisa::arg::LogLevel> app{"Executes commands for shards."};
    auto* invert =
        app.add_subcommand("invert", "Constructs an inverted index from a forward index.");
    auto* reorder = app.add_subcommand("reorder-docids", "Reorder document IDs.");
    auto* compress = app.add_subcommand("compress", "Compresses an inverted index");
    auto* wand = app.add_subcommand("wand-data", "Creates additional data for query processing.");
    auto* taily = app.add_subcommand(
        "taily-stats", "Extracts Taily statistics from the index and stores it in a file.");
    auto* taily_rank = app.add_subcommand(
        "taily-score",
        "Computes Taily shard ranks for queries."
        " NOTE: as term IDs need to be resolved individually for each shard,"
        " DO NOT provide already parsed and resolved queries (with IDs instead of terms).");
    auto* taily_thresholds = app.add_subcommand("taily-thresholds", "Computes Taily thresholds.");
    InvertArgs invert_args(invert);
    ReorderDocuments reorder_args(reorder);
    CompressArgs compress_args(compress);
    CreateWandDataArgs wand_args(wand);
    TailyStatsArgs taily_args(taily);
    TailyRankArgs taily_rank_args(taily_rank);
    TailyThresholds taily_thresholds_args(taily_thresholds);
    app.require_subcommand(1);
    CLI11_PARSE(app, argc, argv);

    spdlog::set_level(app.log_level());

    try {
        if (invert->parsed()) {
            tbb::global_control control(
                tbb::global_control::max_allowed_parallelism, invert_args.threads() + 1);
            spdlog::info("Number of worker threads: {}", invert_args.threads());
            Shard_Id shard_id{0};

            InvertParams params;
            params.batch_size = invert_args.batch_size();
            params.num_threads = invert_args.threads();

            for (auto shard: resolve_shards(invert_args.input_basename())) {
                invert::invert_forward_index(
                    format_shard(invert_args.input_basename(), shard_id),
                    format_shard(invert_args.output_basename(), shard_id),
                    params);
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
        if (taily->parsed()) {
            auto shards = resolve_shards(taily_args.collection_path(), ".docs");
            spdlog::info("Processing {} shards", shards.size());
            for (auto shard: shards) {
                auto shard_args = taily_args;
                shard_args.apply_shard(shard);
                pisa::extract_taily_stats(shard_args);
            }
        }
        if (taily_rank->parsed()) {
            auto shards = resolve_shards(taily_rank_args.shard_stats());
            pisa::VecMap<Shard_Id, std::string> shard_stats;
            std::vector<::pisa::Query> global_queries;
            pisa::VecMap<Shard_Id, std::vector<::pisa::Query>> shard_queries;
            for (auto shard: shards) {
                auto shard_args = taily_rank_args;
                shard_args.apply_shard(shard);
                shard_stats.push_back(shard_args.shard_stats());
                shard_queries.push_back(shard_args.queries());
            }
            pisa::taily_score_shards(
                taily_rank_args.global_stats(),
                shard_stats,
                taily_rank_args.queries(),
                shard_queries,
                taily_rank_args.k(),
                print_taily_scores);
        }
        if (taily_thresholds->parsed()) {
            auto shards = resolve_shards(taily_thresholds_args.stats());
            spdlog::info("Processing {} shards", shards.size());
            for (auto shard: shards) {
                auto shard_args = taily_thresholds_args;
                shard_args.apply_shard(shard);
                pisa::estimate_taily_thresholds(shard_args);
            }
        }
        return 0;
    } catch (pisa::io::NoSuchFile const& err) {
        spdlog::error("{}", err.what());
        return 1;
    } catch (std::exception const& err) {
        spdlog::error("{}", err.what());
    } catch (...) {
        spdlog::error("Unknown error occurred.");
    }
}
