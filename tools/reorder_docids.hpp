#pragma once

#include "app.hpp"
#include "pisa/reorder_docids.hpp"
#include "tbb/task_scheduler_init.h"

namespace pisa {

auto reorder_docids(ReorderDocuments args) -> int
{
    tbb::task_scheduler_init init(args.threads());
    try {
        if (args.bp()) {
            return recursive_graph_bisection(RecursiveGraphBisectionOptions{
                .input_basename = args.input_basename(),
                .output_basename = args.output_basename(),
                .output_fwd = args.output_fwd(),
                .input_fwd = args.input_fwd(),
                .document_lexicon = args.document_lexicon(),
                .reordered_document_lexicon = args.reordered_document_lexicon(),
                .depth = args.depth(),
                .node_config = args.node_config(),
                .min_length = args.min_length(),
                .compress_fwd = not args.nogb(),
                .print_args = args.print(),
            });
        }
        ReorderOptions options{.input_basename = args.input_basename(),
                               .output_basename = *args.output_basename(),
                               .document_lexicon = args.document_lexicon(),
                               .reordered_document_lexicon = args.reordered_document_lexicon()};
        if (args.random()) {
            return reorder_random(options, args.seed());
        }
        if (auto feature_file = args.feature_file(); feature_file) {
            return reorder_by_feature(options, *feature_file);
        }
        if (auto input = args.mapping_file(); input) {
            return reorder_from_mapping(options, *input);
        }
    } catch (std::invalid_argument& err) {
        spdlog::error("{}", err.what());
        std::exit(1);
    }
    spdlog::error("Should be unreachable due to argument constraints!");
    std::exit(1);
}

}  // namespace pisa
