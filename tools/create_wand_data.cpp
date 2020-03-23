#include <fstream>
#include <iostream>
#include <unordered_set>

#include "boost/variant.hpp"
#include "spdlog/spdlog.h"

#include "binary_collection.hpp"
#include "binary_freq_collection.hpp"
#include "mappable/mapper.hpp"
#include "util/util.hpp"
#include "wand_data.hpp"
#include "wand_data_compressed.hpp"
#include "wand_data_range.hpp"
#include "wand_data_raw.hpp"

#include "CLI/CLI.hpp"

int main(int argc, const char** argv)
{
    using namespace pisa;

    std::optional<float> lambda{};
    std::optional<uint64_t> fixed_block_size{};
    std::string input_basename;
    std::string output_filename;
    std::string scorer_name;
    bool compress = false;
    bool range = false;
    bool quantize = false;
    std::string terms_to_drop_filename;

    CLI::App app{"create_wand_data - a tool for creating additional data for query processing."};
    app.add_option("-c,--collection", input_basename, "Collection basename")->required();
    app.add_option("-o,--output", output_filename, "Output filename")->required();
    auto block_group = app.add_option_group("blocks");
    auto block_size_opt = block_group->add_option(
        "-b,--block-size", fixed_block_size, "Block size for fixed-length blocks");
    auto block_lambda_opt =
        block_group->add_option("-l,--lambda", lambda, "Lambda parameter for variable blocks")
            ->excludes(block_size_opt);
    block_group->require_option();

    app.add_flag("--compress", compress, "Compress additional data");
    app.add_flag("--quantize", quantize, "Quantize scores");
    app.add_option("-s,--scorer", scorer_name, "Scorer function")->required();
    app.add_flag("--range", range, "Create docid-range based data")
        ->excludes(block_size_opt)
        ->excludes(block_lambda_opt);
    app.add_option(
        "--terms-to-drop",
        terms_to_drop_filename,
        "A filename containing a list of term IDs that we want to drop");

    CLI11_PARSE(app, argc, argv);

    std::string partition_type_name = (lambda) ? "variable partition" : "static partition";
    spdlog::info("Block based wand creation with {}", partition_type_name);

    binary_collection sizes_coll((input_basename + ".sizes").c_str());
    binary_freq_collection coll(input_basename.c_str());

    std::ifstream dropped_terms_file(terms_to_drop_filename);
    std::unordered_set<size_t> dropped_term_ids;
    copy(
        istream_iterator<size_t>(dropped_terms_file),
        istream_iterator<size_t>(),
        inserter(dropped_term_ids, dropped_term_ids.end()));

    spdlog::info("Dropping {} terms", dropped_term_ids.size());

    auto const block_size = [&]() -> BlockSize {
        if (lambda) {
            spdlog::info("Lambda {}", *lambda);
            return VariableBlock(*lambda);
        }
        spdlog::info("Fixed block size: {}", *fixed_block_size);
        return FixedBlock(*fixed_block_size);
    }();

    if (compress) {
        wand_data<wand_data_compressed<>> wdata(
            sizes_coll.begin()->begin(),
            coll.num_docs(),
            coll,
            scorer_name,
            block_size,
            quantize,
            dropped_term_ids);
        mapper::freeze(wdata, output_filename.c_str());
    } else if (range) {
        wand_data<wand_data_range<128, 1024>> wdata(
            sizes_coll.begin()->begin(),
            coll.num_docs(),
            coll,
            scorer_name,
            block_size,
            quantize,
            dropped_term_ids);
        mapper::freeze(wdata, output_filename.c_str());
    } else {
        wand_data<wand_data_raw> wdata(
            sizes_coll.begin()->begin(),
            coll.num_docs(),
            coll,
            scorer_name,
            block_size,
            quantize,
            dropped_term_ids);
        mapper::freeze(wdata, output_filename.c_str());
    }
}
