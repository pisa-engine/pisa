#include <algorithm>
#include <thread>
#include <vector>

#include "CLI/CLI.hpp"
#include "gsl/span"
#include "spdlog/spdlog.h"
#include "tbb/global_control.h"
#include "tbb/task_group.h"

#include "app.hpp"
#include "binary_collection.hpp"
#include "invert.hpp"
#include "util/util.hpp"

int main(int argc, char** argv)
{
    CLI::App app{"Constructs an inverted index from a forward index."};
    pisa::InvertArgs args(&app);
    CLI11_PARSE(app, argc, argv);
    tbb::global_control control(tbb::global_control::max_allowed_parallelism, args.threads() + 1);
    spdlog::set_level(args.log_level());
    spdlog::info("Number of worker threads: {}", args.threads());
    try {
        pisa::invert::InvertParams params;
        params.batch_size = args.batch_size();
        params.num_threads = args.threads();
        params.term_count = args.term_count();
        pisa::invert::invert_forward_index(args.input_basename(), args.output_basename(), params);
        return 0;
    } catch (pisa::io::NoSuchFile const& err) {
        spdlog::error("{}", err.what());
        return 1;
    }
}
