#include <thread>
#include "CLI/CLI.hpp"
#include "pstl/execution"
#include "tbb/task_scheduler_init.h"

#include "recursive_graph_bisection.hpp"
#include "util/progress.hpp"

int main(int argc, char const *argv[]) {

    std::string input_basename;
    std::string output_basename;
    std::string input_fwd;
    std::string output_fwd;
    size_t      min_len = 0;
    size_t      depth   = 0;
    size_t      threads = std::thread::hardware_concurrency();
    bool        nogb    = false;
    size_t      prelim  = 0;

    CLI::App app{"Recursive graph bisection algorithm used for inverted indexed reordering."};
    app.add_option("-c,--collection", input_basename, "Collection basename")->required();
    app.add_option("-o,--output", output_basename, "Output basename");
    app.add_option("--store-fwdidx", output_fwd, "Output basename (forward index)");
    app.add_option("--fwdidx", input_fwd, "Use this forward index");
    app.add_option("-m,--min-len", min_len, "Minimum list threshold");
    app.add_option("-d,--depth", depth, "Recursion depth");
    app.add_option("-t,--threads", threads, "Thread count");
    app.add_option("--prelim", prelim, "Precomputing limit");
    app.add_flag("--nogb", nogb, "No VarIntGB compression in forward index");
    CLI11_PARSE(app, argc, argv);

    if (app.count("--output") + app.count("--store-fwdidx") == 0u) {
        std::cerr << "ERROR: Must define at least one output parameter.\n";
        return 1;
    }

    tbb::task_scheduler_init init(threads);

    using namespace ds2i;

    forward_index fwd =
        app.count("--fwdidx") > 0u
            ? forward_index::read(input_fwd)
            : forward_index::from_inverted_index(input_basename, min_len, not nogb);
    if (app.count("--store-fwdidx") > 0u) {
        forward_index::write(fwd, output_fwd);
    }

    if (app.count("--output")) {
        std::vector<uint32_t> documents(fwd.size());
        std::iota(documents.begin(), documents.end(), 0u);
        std::vector<double> gains(fwd.size(), 0.0);
        document_range<std::vector<uint32_t>::iterator> initial_range(
            documents.begin(), documents.end(), fwd, gains);

        if (depth == 0u) {
            depth = static_cast<size_t>(std::log2(fwd.size()) - 5 );
        }
        std::cerr << "Using max depth " << depth << std::endl;
        std::cout << "Number of threads: " << threads << std::endl;
        std::cout << "Minimum list threshold: " << min_len << std::endl;

        {
            size_t parallel_depth = 4 * std::log2(threads);
            ds2i::progress bp_progress("Graph bisection", initial_range.size() * depth);
            bp_progress.update(0);
            recursive_graph_bisection(initial_range, depth, parallel_depth, depth - 6, bp_progress);
        }

        auto mapping = get_mapping(documents);
        fwd.clear();
        documents.clear();
        reorder_inverted_index(input_basename, output_basename, mapping);
    }
    return 0;
}
