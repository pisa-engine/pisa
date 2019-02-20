#include <algorithm>
#include <random>
#include <thread>
#include <vector>

#include <CLI/CLI.hpp>
#include <fmt/format.h>
#include <fmt/ostream.h>
#include <gsl/span>
#include <pstl/algorithm>
#include <pstl/execution>
#include <pstl/numeric>
#include <range/v3/view/chunk.hpp>
#include <range/v3/view/enumerate.hpp>
#include <range/v3/view/zip.hpp>
#include <spdlog/spdlog.h>
#include <tbb/task_scheduler_init.h>

#include "binary_collection.hpp"
#include "io.hpp"
#include "sharding.hpp"
#include "util/util.hpp"
#include "vector.hpp"

using namespace pisa;
using ranges::view::chunk;
using ranges::view::iota;
using ranges::view::zip;

int main(int argc, char **argv) {

    std::string input_basename;
    std::string output_basename;
    std::vector<std::string> shard_files;
    int threads = std::thread::hardware_concurrency();
    int shard_count;

    CLI::App app{"Partition a forward index"};
    app.add_option("-i,--input", input_basename, "Forward index filename")->required();
    app.add_option("-o,--output", output_basename, "Basename of partitioned shards")->required();
    app.add_option("-j,--threads", threads, "Thread count");
    auto random_option
        = app.add_option("-r,--random-shards", shard_count, "Number of random shards");
    auto shard_files_option
        = app.add_option("-s,--shard-files", shard_files, "List of files with shard titles");
    random_option->excludes(shard_files_option);
    shard_files_option->excludes(random_option);
    CLI11_PARSE(app, argc, argv);

    tbb::task_scheduler_init init(threads);
    spdlog::info("Number of threads: {}", threads);

    if (*random_option) {
        auto mapping = create_random_mapping(input_basename, shard_count);
        partition_fwd_index(input_basename, output_basename, mapping);
    }
    else if (*shard_files_option) {
        auto mapping = mapping_from_files(fmt::format("{}.documents", input_basename),
                                          gsl::make_span(shard_files));
        partition_fwd_index(input_basename, output_basename, mapping);
    }
    else {
        spdlog::error("You must define either --random-shards or --shard-files");
        std::exit(1);
    }

    return 0;
}