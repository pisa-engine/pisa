#include <algorithm>
#include <fstream>
#include <iostream>
#include <numeric>
#include <random>
#include <thread>

#include "mappable/mapper.hpp"
#include "spdlog/spdlog.h"

#include "binary_freq_collection.hpp"
#include "util/index_build_utils.hpp"
#include "util/util.hpp"

int main(int argc, const char** argv)
{
    using namespace pisa;

    if (argc != 2) {
        std::cerr << "Usage: " << argv[0] << " <collection basename>" << std::endl;
        return 1;
    }

    const std::string input_basename = argv[1];
    binary_freq_collection input(input_basename.c_str());

    spdlog::info("Computing statistics about document ID space");

    std::vector<float> log2_data(256);
    for (size_t i = 0; i < 256; ++i) {
        log2_data[i] = log2f(i);
    }

    double all_log_gaps = 0.0F;
    size_t no_gaps = 0;
    for (const auto& seq: input) {
        no_gaps += seq.docs.size();
        all_log_gaps += log2f(seq.docs.begin()[0] + 1);
        for (size_t i = 1; i < seq.docs.size(); ++i) {
            auto gap = seq.docs.begin()[i] - seq.docs.begin()[i - 1];
            if (gap < 256) {
                all_log_gaps += log2_data[gap];
            } else {
                all_log_gaps += log2f(gap);
            }
        }
    }
    double average_log_gap = all_log_gaps / no_gaps;
    spdlog::info("Average LogGap of documents: {}", average_log_gap);
}
