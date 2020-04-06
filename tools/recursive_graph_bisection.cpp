#include <numeric>
#include <optional>
#include <thread>

#include <CLI/CLI.hpp>
#include <pstl/execution>
#include <spdlog/spdlog.h>
#include <tbb/task_scheduler_init.h>

#include "app.hpp"
#include "payload_vector.hpp"
#include "pisa/recursive_graph_bisection.hpp"
#include "recursive_graph_bisection.hpp"
#include "util/inverted_index_utils.hpp"
#include "util/progress.hpp"

int main(int argc, char const* argv[])
{
    CLI::App app{"Recursive graph bisection algorithm used for inverted indexed reordering."};
    pisa::RecursiveGraphBisectionArgs args(&app);
    CLI11_PARSE(app, argc, argv);

    if (not args.output_basename() && not args.output_fwd()) {
        spdlog::error("Must define at least one output parameter.");
        return 1;
    }

    tbb::task_scheduler_init init(args.threads());
    spdlog::info("Number of threads: {}", args.threads());
    return pisa::bp::run(args);
}
