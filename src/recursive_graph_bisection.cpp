#include <numeric>
#include <thread>
#include "CLI/CLI.hpp"
#include "pstl/execution"
#include "tbb/task_scheduler_init.h"

#include "recursive_graph_bisection.hpp"
#include "util/progress.hpp"

using namespace pisa;
using iterator_type = std::vector<uint32_t>::iterator;
using range_type    = document_range<iterator_type>;
using node_type     = computation_node<iterator_type>;

inline std::vector<node_type> read_node_config(const std::string &config_file,
                                               const range_type & initial_range) {
    std::vector<node_type> nodes;
    std::ifstream          is(config_file);
    std::string            line;
    while (std::getline(is, line)) {
        std::istringstream iss(line);
        nodes.push_back(node_type::from_stream(iss, initial_range));
    }
    return nodes;
}

inline void run_with_config(const std::string &config_file, const range_type &initial_range) {
    auto nodes       = read_node_config(config_file, initial_range);
    auto total_count = std::accumulate(
        nodes.begin(), nodes.end(), std::ptrdiff_t(0), [](auto acc, const auto &node) {
            return acc + node.partition.size();
        });
    pisa::progress bp_progress("Graph bisection", total_count);
    bp_progress.update(0);
    recursive_graph_bisection(std::move(nodes), bp_progress);
}

inline void run_default_tree(size_t depth, const range_type &initial_range) {
    std::cerr << "Default tree with depth " << depth << std::endl;
    pisa::progress bp_progress("Graph bisection", initial_range.size() * depth);
    bp_progress.update(0);
    recursive_graph_bisection(initial_range, depth, depth - 6, bp_progress);
}

int main(int argc, char const *argv[]) {

    std::string input_basename;
    std::string output_basename;
    std::string input_fwd;
    std::string output_fwd;
    std::string config_file;
    size_t      min_len = 0;
    size_t      depth   = 0;
    size_t      threads = std::thread::hardware_concurrency();
    bool        nogb    = false;
    bool        print   = false;
    size_t      prelim  = 0;

    CLI::App app{"Recursive graph bisection algorithm used for inverted indexed reordering."};
    app.add_option("-c,--collection", input_basename, "Collection basename")->required();
    app.add_option("-o,--output", output_basename, "Output basename");
    app.add_option("--store-fwdidx", output_fwd, "Output basename (forward index)");
    app.add_option("--fwdidx", input_fwd, "Use this forward index");
    app.add_option("-m,--min-len", min_len, "Minimum list threshold");
    auto optdepth =
        app.add_option("-d,--depth", depth, "Recursion depth")->check(CLI::Range(1, 64));
    app.add_option("-t,--threads", threads, "Thread count");
    app.add_option("--prelim", prelim, "Precomputing limit");
    auto optconf = app.add_option("--config", config_file, "Node configuration file");
    app.add_flag("--nogb", nogb, "No VarIntGB compression in forward index");
    app.add_flag("-p,--print", print, "Print ordering to standard output");
    optconf->excludes(optdepth);
    CLI11_PARSE(app, argc, argv);

    bool config_provided = app.count("--config") > 0u;
    bool depth_provided  = app.count("--depth") > 0u;
    bool output_provided  = app.count("--output") > 0u;
    if (app.count("--output") + app.count("--store-fwdidx") == 0u) {
        std::cerr << "ERROR: Must define at least one output parameter.\n";
        return 1;
    }

    tbb::task_scheduler_init init(threads);
    std::cerr << "Number of threads: " << threads << std::endl;

    forward_index fwd = app.count("--fwdidx") > 0u
                            ? forward_index::read(input_fwd)
                            : forward_index::from_inverted_index(input_basename, min_len, not nogb);
    if (app.count("--store-fwdidx") > 0u) {
        forward_index::write(fwd, output_fwd);
    }

    if (output_provided) {
        std::vector<uint32_t> documents(fwd.size());
        std::iota(documents.begin(), documents.end(), 0u);
        std::vector<double> gains(fwd.size(), 0.0);
        range_type initial_range(documents.begin(), documents.end(), fwd, gains);

        if (config_provided) {
            run_with_config(config_file, initial_range);
        } else {
            run_default_tree(depth_provided ? depth
                                            : static_cast<size_t>(std::log2(fwd.size()) - 5),
                             initial_range);
        }

        if (print) {
            for (const auto& document : documents) {
                std::cout << document << '\n';
            }
        }
        auto mapping = get_mapping(documents);
        fwd.clear();
        documents.clear();
        reorder_inverted_index(input_basename, output_basename, mapping);
    }
    return 0;
}
