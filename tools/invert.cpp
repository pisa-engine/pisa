#include <algorithm>
#include <thread>
#include <vector>

#include "CLI/CLI.hpp"
#include "gsl/span"
#include "pstl/algorithm"
#include "pstl/execution"
#include "spdlog/spdlog.h"
#include "tbb/task_group.h"
#include "tbb/task_scheduler_init.h"

#include "app.hpp"
#include "binary_collection.hpp"
#include "invert.hpp"
#include "util/util.hpp"

int main(int argc, char** argv)
{
    CLI::App app{"Constructs an inverted index from a forward index."};
    pisa::InvertArgs args(&app);
    CLI11_PARSE(app, argc, argv);
    tbb::task_scheduler_init init(args.threads());
    spdlog::info("Number of threads: {}", args.threads());
    pisa::invert::invert_forward_index(
        args.input_basename(),
        args.output_basename(),
        args.batch_size(),
        args.threads(),
        args.term_count());
    return 0;
}
