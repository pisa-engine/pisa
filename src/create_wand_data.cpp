#include <fstream>
#include <iostream>

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

int main(int argc, const char **argv)
{
    using namespace pisa;

    std::optional<float> lambda{};
    std::optional<uint64_t> fixed_block_size{};
    std::string input_basename;
    std::string output_filename;
    bool variable_block = false;
    bool compress = false;
    bool range = false;

    CLI::App app{"create_wand_data - a tool for creating additional data for query processing."};
    app.add_option("-c,--collection", input_basename, "Collection basename")->required();
    app.add_option("-o,--output", output_filename, "Output filename")->required();
    auto var_block_opt = app.add_flag("--variable-block", variable_block, "Variable length blocks");
    auto var_block_param_opt =
        app.add_option("-b,--block-size", fixed_block_size, "Block size for fixed-length blocks")
            ->excludes(var_block_opt);
    app.add_option("-l,--lambda", lambda, "Lambda parameter for variable blocks")
        ->excludes(var_block_param_opt)
        ->needs(var_block_opt);
    app.add_flag("--compress", compress, "Compress additional data");
    app.add_flag("--range", range, "Create docid-range based data")->excludes(var_block_opt);

    CLI11_PARSE(app, argc, argv);

    std::string partition_type_name = (lambda) ? "variable partition" : "static partition";
    spdlog::info("Block based wand creation with {}", partition_type_name);

    binary_collection sizes_coll((input_basename + ".sizes").c_str());
    binary_freq_collection coll(input_basename.c_str());

    auto const block_size = [&]() -> BlockSize {
        if (variable_block) {
            return fixed_block_size ? FixedBlock(*fixed_block_size) : FixedBlock();
        } else {
            return lambda ? VariableBlock(*lambda) : VariableBlock();
        }
    }();

    if (compress) {
        wand_data<bm25, wand_data_compressed<bm25, uniform_score_compressor>> wdata(
            sizes_coll.begin()->begin(), coll.num_docs(), coll, block_size);
        mapper::freeze(wdata, output_filename.c_str());
    } else if (range) {
        wand_data<bm25, wand_data_range<128, 1024, bm25>> wdata(
            sizes_coll.begin()->begin(), coll.num_docs(), coll, block_size);
        mapper::freeze(wdata, output_filename.c_str());
    } else {
        wand_data<bm25, wand_data_raw<bm25>> wdata(
            sizes_coll.begin()->begin(), coll.num_docs(), coll, block_size);
        mapper::freeze(wdata, output_filename.c_str());
    }
}
