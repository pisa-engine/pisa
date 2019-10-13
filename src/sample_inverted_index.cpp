#include <cmath>
#include <fstream>
#include <random>
#include <string>

#include "CLI/CLI.hpp"
#include "binary_freq_collection.hpp"
#include "invert.hpp"
#include "util/progress.hpp"

int main(int argc, char **argv)
{

    using namespace pisa;
    std::string input_basename;
    std::string output_basename;
    float rate;
    unsigned seed = std::random_device{}();

    CLI::App app{"A tool for sampling an inverted index."};
    app.add_option("-c,--collection", input_basename, "Input collection basename")->required();
    app.add_option("-o,--output", output_basename, "Output collection basename")->required();
    app.add_option("-r,--rate", rate, "Sampling rate (proportional size of the output index)")->required();
    app.add_option("--seed", seed, "Seed state");
    CLI11_PARSE(app, argc, argv);

    if (rate < 0 or rate > 1) {
        spdlog::error("Sampling rate should be between 0 and 1.");
        std::abort();
    }
    binary_freq_collection input(input_basename.c_str());

    boost::filesystem::copy_file(fmt::format("{}.sizes", input_basename),
                                 fmt::format("{}.sizes", output_basename),
                                 boost::filesystem::copy_option::overwrite_if_exists);

    std::ofstream dos(output_basename + ".docs");
    std::ofstream fos(output_basename + ".freqs");

    auto document_count = static_cast<uint32_t>(input.num_docs());
    write_sequence(dos, gsl::make_span<uint32_t const>(&document_count, 1));

    {
        pisa::progress progress("Sampling inverted index", input.size());
        for (auto const &plist : input) {
            std::vector<uint32_t> docs(plist.docs.begin(), plist.docs.end());
            std::vector<uint32_t> freqs(plist.freqs.begin(), plist.freqs.end());

            std::mt19937 eng1(seed);
            auto eng2 = eng1;
            std::shuffle(docs.begin(), docs.end(), eng1);
            std::shuffle(freqs.begin(), freqs.end(), eng2);
            size_t new_size = std::ceil(plist.docs.size() * rate);
            docs.resize(new_size);
            freqs.resize(new_size);
            sort(docs.begin(), docs.end());
            sort(freqs.begin(), freqs.end());
            write_sequence(dos, gsl::span<uint32_t const>(docs));
            write_sequence(fos, gsl::span<uint32_t const>(freqs));
            progress.update(1);
        }
    }
    dos.close();
    fos.close();

    return 0;
}