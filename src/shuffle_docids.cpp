#include <fstream>
#include <iostream>
#include <algorithm>
#include <thread>
#include <numeric>
#include <random>

#include "succinct/mapper.hpp"

#include "binary_freq_collection.hpp"
#include "util/index_build_utils.hpp"
#include "util/util.hpp"

using ds2i::logger;

int main(int argc, const char** argv)
{

    using namespace ds2i;

    if (argc < 3) {
        std::cerr << "Usage: " << argv[0]
                  << " <collection basename> <output basename>"
                  << std::endl;
        return 1;
    }

    constexpr uint32_t seed = 1729;
    std::mt19937 rng(seed);

    const std::string input_basename = argv[1];
    const std::string output_basename = argv[2];
    binary_freq_collection input(input_basename.c_str());

    logger() << "Computing random permutation" << std::endl;
    size_t num_docs = input.num_docs();
    std::vector<uint32_t> new_doc_id(num_docs);
    std::iota(new_doc_id.begin(), new_doc_id.end(), uint32_t());
    std::shuffle(new_doc_id.begin(), new_doc_id.end(), rng);
    reorder_inverted_index(input_basename, output_basename, new_doc_id);
}
