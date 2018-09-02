#include "CLI/CLI.hpp"

#include "util/index_build_utils.hpp"
#include "util/progress.hpp"

int main(int argc, char const *argv[]) {

    std::string input_basename;
    std::string output_basename;
    size_t      num_docs;

    CLI::App app{"Sample n documents and store in a separate inverted index."};
    app.add_option("-c,--collection", input_basename, "Collection basename")->required();
    app.add_option("-o,--output", output_basename, "Output basename")->required();
    app.add_option("-n,--num-doc", num_docs, "Number of documents")->required();
    CLI11_PARSE(app, argc, argv);

    ds2i::sample_inverted_index(input_basename, output_basename, num_docs);
    return 0;
}
