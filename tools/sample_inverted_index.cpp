#include <cmath>
#include <fstream>
#include <random>
#include <string>
#include <unordered_set>

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
    std::string type;
    std::string terms_to_drop_filename;
    std::optional<std::string> documents_filename;

    std::optional<float> rate;
    unsigned seed = std::random_device{}();

    CLI::App app{"A tool for sampling an inverted index."};
    app.add_option("-c,--collection", input_basename, "Input collection basename")->required();
    app.add_option("-o,--output", output_basename, "Output collection basename")->required();

    auto type_opt = app.add_option_group("sampling_type", "Sampling type");
    auto r_opt = type_opt->add_option("-t,--type", type, "Sampling type");
    auto rate_opt = app.add_option("-r,--rate", rate, "Sampling rate (proportional size of the output index)")->needs(r_opt);
    app.add_option("--seed", seed, "Seed state")->needs(rate_opt);

    type_opt->add_option("-d,--docs", documents_filename, "Document IDs filename");
    type_opt->require_option(1);

    app.add_option("--terms-to-drop",
                   terms_to_drop_filename,
                   "A filename containing a list of term IDs that we want to drop");

    CLI11_PARSE(app, argc, argv);

    if (rate and (*rate <= 0 or *rate > 1)) {
        spdlog::error("Sampling rate should be greater than 0 and lower than or equal to 1.");
        std::abort();
    }
    std::function<std::vector<std::uint32_t>(const binary_collection::const_sequence &docs)>
        sampling_fn;

    if (type == "random_postings") {
        sampling_fn = [&](const auto &docs) {
            size_t sample_size = std::ceil(docs.size() * (*rate));
            std::vector<std::uint32_t> indices(docs.size());
            std::vector<std::uint32_t> sample;
            std::iota(indices.begin(), indices.end(), 0);
            std::sample(indices.begin(),
                        indices.end(),
                        std::back_inserter(sample),
                        sample_size,
                        std::mt19937{seed});

            return sample;
        };
    } else if (type == "random_docids") {
        binary_freq_collection input(input_basename.c_str());
        auto num_docs = input.num_docs();
        size_t sample_size = std::ceil(num_docs * (*rate));
        spdlog::info("Taking {}/{}.", sample_size, num_docs);
        std::vector<std::uint32_t> indices(num_docs);
        std::iota(indices.begin(), indices.end(), 0);
        std::vector<std::uint32_t> sampled_indices;
        std::sample(indices.begin(),
                    indices.end(),
                    std::back_inserter(sampled_indices),
                    sample_size,
                    std::mt19937{seed});
        std::vector<bool> doc_ids(num_docs);
        for (auto &&p : sampled_indices) {
            doc_ids[p] = true;
        }

        sampling_fn = [=](const auto &docs) {
            std::vector<std::uint32_t> sample;
            for (int position = 0; position < docs.size(); ++position) {
                if (doc_ids[*(docs.begin() + position)]) {
                    sample.push_back(position);
                }
            }
            return sample;
        };
    } else if (documents_filename) {
        std::ifstream documents_file(*documents_filename);
        std::unordered_set<size_t> document_ids;
        copy(std::istream_iterator<size_t>(documents_file),
             std::istream_iterator<size_t>(),
             inserter(document_ids, document_ids.end()));

        spdlog::info("Keeping {} documents", document_ids.size());
        sampling_fn = [=](const auto &docs) {
            std::vector<std::uint32_t> sample;
            for (int position = 0; position < docs.size(); ++position) {
                if (document_ids.count(*(docs.begin() + position))) {
                    sample.push_back(position);
                }
            }
            return sample;
        };

    } else {
        spdlog::error("Unknown type {}", type);
        std::abort();
    }
    std::unordered_set<size_t> terms_to_drop;
    sample_inverted_index(input_basename, output_basename, sampling_fn, terms_to_drop);
    std::ofstream dropped_terms_file(terms_to_drop_filename);
    for (const auto &id : terms_to_drop) {
        dropped_terms_file << id << std::endl;
    }

    return 0;
}