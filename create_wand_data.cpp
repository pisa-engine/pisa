#include <fstream>
#include <iostream>

#include "succinct/mapper.hpp"
#include "binary_freq_collection.hpp"
#include "binary_collection.hpp"
#include "wand_data.hpp"
#include "util.hpp"

int main(int argc, const char** argv) {

    using namespace ds2i;

    if (argc != 3) {
        std::cerr << "Usage: " << argv[0]
                  << " <collection basename> <output filename>"
                  << std::endl;
        return 1;
    }

    std::string input_basename = argv[1];
    const char* output_filename = argv[2];

    binary_collection sizes_coll((input_basename + ".sizes").c_str());
    binary_freq_collection coll(input_basename.c_str());

    wand_data<> wdata(sizes_coll.begin()->begin(), coll.num_docs(), coll);
    succinct::mapper::freeze(wdata, output_filename);
}
