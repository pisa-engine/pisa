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
    int threads = std::thread::hardware_concurrency();
    int shard_count;

    CLI::App app{"Partition a forward index"};
    app.add_option("-i,--input", input_basename, "Forward index filename")->required();
    app.add_option("-o,--output", output_basename, "Basename of partitioned shards")->required();
    app.add_option("-j,--threads", threads, "Thread count");
    app.add_option("-s,--shards", shard_count, "Number of shards");
    CLI11_PARSE(app, argc, argv);

    tbb::task_scheduler_init init(threads);
    spdlog::info("Number of threads: {}", threads);
    auto mapping = create_random_mapping(input_basename, shard_count);
    partition_fwd_index(input_basename, output_basename, mapping);

    return 0;
}
