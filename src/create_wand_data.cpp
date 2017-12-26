#include <fstream>
#include <iostream>

#include "binary_collection.hpp"
#include "binary_freq_collection.hpp"
#include "succinct/mapper.hpp"
#include "util/util.hpp"
#include "wand_data.hpp"
#include "wand_data_compressed.hpp"
#include "wand_data_raw.hpp"

namespace {
void printUsage(const std::string &programName) {
  std::cerr << "Usage: " << programName
            << " <collection basename> <output filename>"
            << " [--variable-block]"
            << "[--compress]" << std::endl;
}
} // namespace

int main(int argc, const char **argv) {
  using namespace ds2i;
  std::string programName = argv[0];
  if (argc < 3) {
    printUsage(programName);
    return 1;
  }

  std::string input_basename = argv[1];
  const char *output_filename = argv[2];
  partition_type p_type = partition_type::fixed_blocks;
  bool compress = false;

  for (int i = 3; i < argc; ++i) {
    std::string arg = argv[i];
    if (arg == "--variable-block") {
      p_type = partition_type::variable_blocks;
    } else if (arg == "--compress") {
      compress = true;
    } else {
      printUsage(programName);
      return 1;
    }
  }

  std::string partition_type_name = (p_type == partition_type::fixed_blocks)
                                        ? "static partition"
                                        : "variable partition";
  logger() << "Block based wand creation with " << partition_type_name
           << std::endl;

  binary_collection sizes_coll((input_basename + ".sizes").c_str());
  binary_freq_collection coll(input_basename.c_str());

  if (compress) {
    wand_data<bm25, wand_data_compressed<bm25, uniform_score_compressor>> wdata(
        sizes_coll.begin()->begin(), coll.num_docs(), coll, p_type);
    succinct::mapper::freeze(wdata, output_filename);
  } else {
    wand_data<bm25, wand_data_raw<bm25>> wdata(sizes_coll.begin()->begin(),
                                               coll.num_docs(), coll, p_type);
    succinct::mapper::freeze(wdata, output_filename);
  }
}
