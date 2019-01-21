#include <fstream>
#include <iostream>

#include "spdlog/spdlog.h"

#include "binary_collection.hpp"
#include "binary_freq_collection.hpp"
#include "succinct/mapper.hpp"
#include "util/util.hpp"
#include "wand_data.hpp"
#include "wand_data_compressed.hpp"
#include "wand_data_raw.hpp"

#include "CLI/CLI.hpp"

int main(int argc, const char **argv) {
    using namespace pisa;

    std::string input_basename;
    std::string output_filename;
    bool        variable_block = false;
    bool        compress       = false;

    CLI::App app{"create_wand_data - a tool for creating additional data for query processing."};
    app.add_option("-c,--collection", input_basename, "Collection basename")->required();
    app.add_option("-o,--output", output_filename, "Output filename")->required();
    app.add_flag("--variable-block", variable_block, "Variable length blocks");
    app.add_flag("--compress", compress, "Compress additional data");
    CLI11_PARSE(app, argc, argv);

    partition_type p_type =
        variable_block ? partition_type::variable_blocks : partition_type::fixed_blocks;

    std::string partition_type_name =
        (p_type == partition_type::fixed_blocks) ? "static partition" : "variable partition";
    spdlog::info("Block based wand creation with {}", partition_type_name);

    binary_collection      sizes_coll((input_basename + ".sizes").c_str());
    binary_freq_collection coll(input_basename.c_str());

    if (compress) {
        wand_data<bm25, wand_data_compressed<bm25, uniform_score_compressor>> wdata(
            sizes_coll.begin()->begin(), coll.num_docs(), coll, p_type);
        mapper::freeze(wdata, output_filename.c_str());
    } else {
        wand_data<bm25, wand_data_raw<bm25>> wdata(
            sizes_coll.begin()->begin(), coll.num_docs(), coll, p_type);
        mapper::freeze(wdata, output_filename.c_str());
    }
}
