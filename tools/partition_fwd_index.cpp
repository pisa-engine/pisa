#include <algorithm>
#include <exception>
#include <random>
#include <thread>
#include <vector>

#include <CLI/CLI.hpp>
#include <fmt/format.h>
#include <fmt/ostream.h>
#include <gsl/span>
#include <range/v3/view/chunk.hpp>
#include <range/v3/view/enumerate.hpp>
#include <range/v3/view/iota.hpp>
#include <range/v3/view/zip.hpp>
#include <spdlog/spdlog.h>
#include <tbb/global_control.h>

#include "app.hpp"
#include "binary_collection.hpp"
#include "io.hpp"
#include "sharding.hpp"
#include "util/util.hpp"
#include "vec_map.hpp"

using namespace pisa;
using ranges::views::chunk;
using ranges::views::iota;
using ranges::views::zip;

int main(int argc, char** argv)
{
    std::string input_basename;
    std::string output_basename;
    std::vector<std::string> shard_files;
    int threads = std::thread::hardware_concurrency();
    int shard_count;

    pisa::App<pisa::arg::LogLevel> app{"Partition a forward index"};
    app.add_option("-i,--input", input_basename, "Forward index filename")->required();
    app.add_option("-o,--output", output_basename, "Basename of partitioned shards")->required();
    app.add_option("-j,--threads", threads, "Thread count");
    auto random_option =
        app.add_option("-r,--random-shards", shard_count, "Number of random shards");
    auto shard_files_option =
        app.add_option("-s,--shard-files", shard_files, "List of files with shard titles");
    random_option->excludes(shard_files_option);
    shard_files_option->excludes(random_option);
    CLI11_PARSE(app, argc, argv);

    spdlog::set_level(app.log_level());

    tbb::global_control control(tbb::global_control::max_allowed_parallelism, threads + 1);
    spdlog::info("Number of worker threads: {}", threads);

    try {
        if (*random_option) {
            auto mapping = create_random_mapping(input_basename, shard_count);
            partition_fwd_index(input_basename, output_basename, mapping);
        } else if (*shard_files_option) {
            auto mapping = mapping_from_files(
                fmt::format("{}.documents", input_basename), gsl::make_span(shard_files));
            partition_fwd_index(input_basename, output_basename, mapping);
        } else {
            spdlog::error("You must define either --random-shards or --shard-files");
            std::exit(1);
        }
    } catch (std::exception const& err) {
        spdlog::error("{}", err.what());
        std::exit(1);
    } catch (...) {
        spdlog::error("Unknown error");
        std::exit(1);
    }

    return 0;
}
