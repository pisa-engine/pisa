#include <algorithm>
#include <fstream>
#include <iostream>
#include <numeric>
#include <optional>
#include <random>
#include <thread>

#include <CLI/CLI.hpp>
#include <spdlog/spdlog.h>

#include "mappable/mapper.hpp"
#include "payload_vector.hpp"

#include "binary_freq_collection.hpp"
#include "util/index_build_utils.hpp"
#include "util/inverted_index_utils.hpp"
#include "util/progress.hpp"
#include "util/util.hpp"

int main(int argc, const char** argv)
{
    using namespace pisa;
    std::string input_basename;
    std::string output_basename;
    std::optional<std::string> mapping_filename;
    std::optional<std::string> documents_filename;
    std::optional<std::string> reordered_documents_filename;
    unsigned seed = std::random_device{}();

    CLI::App app{"Shuffle document IDs."};
    app.add_option("-c,--collection", input_basename, "Collection basename")->required();
    app.add_option("-o,--output", output_basename, "Output basename");
    auto mapping_opt = app.add_option(
        "--mapping-filename",
        mapping_filename,
        "Ordering file is of the form <current ID> <new ID>");
    app.add_option("--seed", seed, "Seed state")->excludes(mapping_opt);
    auto docs_opt = app.add_option("--documents", documents_filename, "Documents lexicon");
    app.add_option(
           "--reordered-documents", reordered_documents_filename, "Reordered documents lexicon")
        ->needs(docs_opt);
    CLI11_PARSE(app, argc, argv);

    binary_freq_collection input(input_basename.c_str());
    size_t num_docs = input.num_docs();
    std::vector<uint32_t> mapping(num_docs);

    if (mapping_filename) {
        std::ifstream in_order(*mapping_filename);
        uint32_t prev_id, new_id;
        size_t count = 0;
        while (in_order >> prev_id >> new_id) {
            mapping[prev_id] = new_id;
            ++count;
        }
        if (mapping.size() != count) {
            throw std::invalid_argument("Invalid document order file.");
        }
    }

    else {
        spdlog::info("Computing random permutation");
        std::iota(mapping.begin(), mapping.end(), uint32_t());
        std::shuffle(mapping.begin(), mapping.end(), std::mt19937{seed});
    }
    {
        spdlog::info("Shuffling document sizes");
        binary_collection input_sizes((input_basename + ".sizes").c_str());
        auto sizes = *input_sizes.begin();
        if (sizes.size() != num_docs) {
            throw std::invalid_argument("Invalid sizes file");
        }

        std::vector<uint32_t> new_sizes(num_docs);
        for (size_t i = 0; i < num_docs; ++i) {
            new_sizes[mapping[i]] = sizes.begin()[i];
        }

        std::ofstream output_sizes(output_basename + ".sizes");
        emit(output_sizes, sizes.size());
        emit(output_sizes, new_sizes.data(), num_docs);
    }

    pisa::progress progress("Shuffling posting lists", input.size());

    std::ofstream output_docs(output_basename + ".docs");
    std::ofstream output_freqs(output_basename + ".freqs");
    emit(output_docs, 1);
    emit(output_docs, num_docs);

    std::vector<std::pair<uint32_t, uint32_t>> pl;
    for (const auto& seq: input) {
        for (size_t i = 0; i < seq.docs.size(); ++i) {
            pl.emplace_back(mapping[seq.docs.begin()[i]], seq.freqs.begin()[i]);
        }

        std::sort(pl.begin(), pl.end());

        emit(output_docs, pl.size());
        emit(output_freqs, pl.size());
        for (const auto& posting: pl) {
            emit(output_docs, posting.first);
            emit(output_freqs, posting.second);
        }

        progress.update(1);
        pl.clear();
    }
    if (documents_filename) {
        auto doc_buffer = Payload_Vector_Buffer::from_file(*documents_filename);
        auto documents = Payload_Vector<std::string>(doc_buffer);
        std::vector<std::string> reordered_documents(documents.size());
        pisa::progress doc_reorder("Reordering documents vector", documents.size());
        for (size_t i = 0; i < documents.size(); ++i) {
            reordered_documents[mapping[i]] = documents[i];
            doc_reorder.update(1);
        }
        encode_payload_vector(reordered_documents.begin(), reordered_documents.end())
            .to_file(*reordered_documents_filename);
    }
}
