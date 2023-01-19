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
#include "util/log.hpp"
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

    pisa::Log2<256> log2;

    double all_log_gaps = 0.0F;
    size_t no_gaps = 0;
    for (const auto& seq: input) {
        no_gaps += seq.docs.size();
        all_log_gaps += log2f(seq.docs.begin()[0] + 1);
        for (size_t i = 1; i < seq.docs.size(); ++i) {
            auto gap = seq.docs.begin()[i] - seq.docs.begin()[i - 1];
            all_log_gaps += log2(gap);
        }
    }
    double average_log_gap = all_log_gaps / no_gaps;
    spdlog::info("Average LogGap of documents: {}", average_log_gap);
}
