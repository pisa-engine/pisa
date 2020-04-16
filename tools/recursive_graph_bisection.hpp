#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include <spdlog/spdlog.h>
#include <tbb/task_group.h>
#include <tbb/task_scheduler_init.h>

#include "app.hpp"
#include "pisa/recursive_graph_bisection.hpp"

namespace pisa::bp {

using iterator_type = std::vector<uint32_t>::iterator;
using range_type = document_range<iterator_type>;
using node_type = computation_node<iterator_type>;

inline std::vector<node_type>
read_node_config(const std::string& config_file, const range_type& initial_range)
{
    std::vector<node_type> nodes;
    std::ifstream is(config_file);
    std::string line;
    while (std::getline(is, line)) {
        std::istringstream iss(line);
        nodes.push_back(node_type::from_stream(iss, initial_range));
    }
    return nodes;
}

inline void run_with_config(const std::string& config_file, const range_type& initial_range)
{
    auto nodes = read_node_config(config_file, initial_range);
    auto total_count = std::accumulate(
        nodes.begin(), nodes.end(), std::ptrdiff_t(0), [](auto acc, const auto& node) {
            return acc + node.partition.size();
        });
    pisa::progress bp_progress("Graph bisection", total_count);
    bp_progress.update(0);
    recursive_graph_bisection(std::move(nodes), bp_progress);
}

inline void run_default_tree(size_t depth, const range_type& initial_range)
{
    spdlog::info("Default tree with depth {}", depth);
    pisa::progress bp_progress("Graph bisection", initial_range.size() * depth);
    bp_progress.update(0);
    recursive_graph_bisection(initial_range, depth, depth - 6, bp_progress);
}

[[nodiscard]] auto run(RecursiveGraphBisectionArgs const& args) -> int
{
    if (not args.output_basename() && not args.output_fwd()) {
        spdlog::error("Must define at least one output parameter.");
        return 1;
    }
    tbb::task_scheduler_init init(args.threads());
    spdlog::info("Number of threads: {}", args.threads());

    forward_index fwd = args.input_fwd()
        ? forward_index::read(*args.input_fwd())
        : forward_index::from_inverted_index(
            args.input_basename(), args.min_length(), not args.nogb());

    if (args.output_fwd()) {
        forward_index::write(fwd, *args.output_fwd());
    }

    if (args.output_basename()) {
        std::vector<uint32_t> documents(fwd.size());
        std::iota(documents.begin(), documents.end(), 0U);
        std::vector<double> gains(fwd.size(), 0.0);
        range_type initial_range(documents.begin(), documents.end(), fwd, gains);

        if (args.node_config()) {
            run_with_config(*args.node_config(), initial_range);
        } else {
            run_default_tree(
                args.depth().value_or(static_cast<size_t>(std::log2(fwd.size()) - 5)), initial_range);
        }

        if (args.print()) {
            for (const auto& document: documents) {
                std::cout << document << '\n';
            }
        }
        auto mapping = get_mapping(documents);
        fwd.clear();
        documents.clear();
        reorder_inverted_index(args.input_basename(), *args.output_basename(), mapping);

        if (args.document_lexicon()) {
            auto doc_buffer = Payload_Vector_Buffer::from_file(*args.document_lexicon());
            auto documents = Payload_Vector<std::string>(doc_buffer);
            std::vector<std::string> reordered_documents(documents.size());
            pisa::progress doc_reorder("Reordering documents vector", documents.size());
            for (size_t i = 0; i < documents.size(); ++i) {
                reordered_documents[mapping[i]] = documents[i];
                doc_reorder.update(1);
            }
            encode_payload_vector(reordered_documents.begin(), reordered_documents.end())
                .to_file(*args.reordered_document_lexicon());
        }
    }
    return 0;
}

}  // namespace pisa::bp
