#include <algorithm>
#include <thread>
#include <vector>

#include <CLI/CLI.hpp>
#include <gsl/span>
#include <pstl/algorithm>
#include <pstl/execution>
#include <spdlog/spdlog.h>
#include <tbb/task_group.h>
#include <tbb/task_scheduler_init.h>

#include "binary_collection.hpp"
#include "invert.hpp"
#include "util/util.hpp"

using namespace pisa;

int main(int argc, char **argv)
{

    std::string input_basename;
    std::string output_basename;
    std::size_t threads = std::thread::hardware_concurrency();
    std::size_t term_count;
    std::ptrdiff_t batch_size = 100'000;

    CLI::App app{"invert - turn forward index into inverted index"};

    app.add_option("-i,--input", input_basename, "Forward index filename")->required();
    app.add_option("-o,--output", output_basename, "Output inverted index basename")->required();
    app.add_option("-j,--threads", threads, "Thread count");
    /// TODO(michal): This should not be required but knowing term count ahead of time makes things
    ///               much simpler. Maybe we can store it in the forward index?
    app.add_option("--term-count", term_count, "Term count")->required();
    app.add_option("-b,--batch-size", batch_size, "Number of documents to process at a time", true);

    std::size_t batch_count;
    bool remove_batches = false;
    auto *merge = app.add_subcommand("merge", "Merge already existing batches");
    merge->add_option("-o,--output", output_basename, "Output inverted index basename")->required();
    merge->add_option("--term-count", term_count, "Term count")->required();
    merge->add_option("--batch-count", batch_count, "Number of batches")->required();
    merge->add_flag("--rm", remove_batches, "Remove batches after merging")->required();

    CLI11_PARSE(app, argc, argv);

    if (merge) {
        invert::merge_batches(output_basename, batch_count, term_count);
        if (remove_batches) {
            invert::remove_batches(output_basename, batch_count, term_count);
        }
    }

    tbb::task_scheduler_init init(threads);
    spdlog::info("Number of threads: {}", threads);

    return 0;
}
