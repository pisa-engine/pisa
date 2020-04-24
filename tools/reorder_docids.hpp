#pragma once

#include "app.hpp"
#include "pisa/reorder_docids.hpp"

namespace pisa {

auto reorder_docids(ReorderDocuments args) -> int
{
    try {
        if (args.bp()) {
            return recursive_graph_bisection(RecursiveGraphBisectionOptions{
                .input_basename = args.input_basename(),
                .output_basename = args.output_basename(),
                .document_lexicon = args.document_lexicon(),
                .reordered_document_lexicon = args.reordered_document_lexicon(),
                .input_fwd = args.input_fwd(),
                .output_fwd = args.output_fwd(),
                .node_config = args.node_config(),
                .depth = args.depth(),
                .compress_fwd = not args.nogb(),
                .print_args = not args.print(),
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
