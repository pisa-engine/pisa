#include <cmath>
#include <fstream>
#include <random>
#include <string>

#include "CLI/CLI.hpp"
#include "binary_freq_collection.hpp"
#include "invert.hpp"
#include "util/inverted_index_utils.hpp"
#include "util/progress.hpp"

using namespace pisa;

int main(int argc, char **argv)
{

    std::string input_basename;
    std::string output_basename;
    float rate;
    unsigned seed = std::random_device{}();

    CLI::App app{"A tool for sampling an inverted index."};
    app.add_option("-c,--collection", input_basename, "Input collection basename")->required();
    app.add_option("-o,--output", output_basename, "Output collection basename")->required();
    app.add_option("-r,--rate", rate, "Sampling rate (proportional size of the output index)")
        ->required();
    app.add_option("--seed", seed, "Seed state");
    CLI11_PARSE(app, argc, argv);

    if (rate <= 0 or rate > 1) {
        spdlog::error("Sampling rate should be greater than 0 and lower than or equal to 1.");
        std::abort();
    }

    auto random_sampling = [&](size_t size) {
        size_t sample_size = std::ceil(size * rate);
        std::vector<std::uint32_t> indices(size);
        std::vector<std::uint32_t> sample;
        std::iota(indices.begin(), indices.end(), 0);
        std::sample(indices.begin(),
                    indices.end(),
                    std::back_inserter(sample),
                    sample_size,
                    std::mt19937{seed});

        return sample;
    };

    sample_inverted_index(input_basename, output_basename, random_sampling);
    return 0;
}