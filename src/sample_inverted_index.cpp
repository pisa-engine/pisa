#include <cmath>
#include <fstream>
#include <random>
#include <string>

#include "CLI/CLI.hpp"
#include "binary_freq_collection.hpp"
#include "invert.hpp"
#include "util/progress.hpp"

using namespace pisa;

template <typename SampleFn>
void sample_index(std::string const &input_basename,
                  std::string const &output_basename,
                  SampleFn &&sample_fn)
{

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

            std::vector<std::uint32_t> sampled_docs;
            std::vector<std::uint32_t> sampled_freqs;
            tie(sampled_docs, sampled_freqs) = sample_fn(docs, freqs);

            write_sequence(dos, gsl::span<uint32_t const>(sampled_docs));
            write_sequence(fos, gsl::span<uint32_t const>(sampled_freqs));
            progress.update(1);
        }
    }
    dos.close();
    fos.close();
}

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

    if (rate < 0 or rate > 1) {
        spdlog::error("Sampling rate should be between 0 and 1.");
        std::abort();
    }

    auto random_sampling = [&](auto &docs, auto &freqs) {
        auto sample_size = std::ceil(docs.size() * rate);
        std::vector<std::uint32_t> indices(docs.size());
        std::vector<std::uint32_t> sample;
        std::iota(indices.begin(), indices.end(), 0);
        std::sample(indices.begin(),
                    indices.end(),
                    std::back_inserter(sample),
                    sample_size,
                    std::mt19937{std::random_device{}()});

        std::vector<std::uint32_t> sampled_docs;
        std::vector<std::uint32_t> sampled_freqs;
        for (auto index : sample) {
            sampled_docs.push_back(docs[index]);
            sampled_freqs.push_back(freqs[index]);
        }
        return std::make_pair(sampled_docs, sampled_freqs);
    };

    sample_index(input_basename, output_basename, random_sampling);
    return 0;
}