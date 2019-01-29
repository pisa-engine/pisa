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

#include "binary_collection.hpp"
#include "invert.hpp"
#include "util/util.hpp"

using namespace pisa;

int main(int argc, char **argv) {

    std::string input_basename;
    std::string output_basename;
    size_t      threads = std::thread::hardware_concurrency();
    size_t      term_count;
    ptrdiff_t   batch_size = 100'000;

    CLI::App app{"invert - turn forward index into inverted index"};
    app.add_option("-i,--input", input_basename, "Forward index filename")->required();
    app.add_option("-o,--output", output_basename, "Output inverted index basename")->required();
    app.add_option("-j,--threads", threads, "Thread count");
    /// TODO(michal): This should not be required but knowing term count ahead of time makes things
    ///               much simpler. Maybe we can store it in the forward index?
    app.add_option("--term-count", term_count, "Term count")->required();
    app.add_option("-b,--batch-size", batch_size, "Number of documents to process at a time", true);
    CLI11_PARSE(app, argc, argv);

    tbb::task_scheduler_init init(threads);
    spdlog::info("Number of threads: {}", threads);
    invert::invert_forward_index(input_basename, output_basename, term_count, batch_size, threads);

    return 0;
}
