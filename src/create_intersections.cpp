#include <string>

#include <CLI/CLI.hpp>

#include "intersection_index.hpp"

int main(int argc, char **argv)
{

    using namespace pisa;
    std::string input_basename;
    std::string output_filename;
    bool check = false;

    CLI::App app{"create_freq_index - a tool for creating an index."};
    app.add_option("-c,--collection", input_basename, "Collection basename")->required();
    app.add_option("-o,--output", output_filename, "Output filename")->required();
    // app.add_flag("--check", check, "Check the correctness of the index");
    CLI11_PARSE(app, argc, argv);

    create_intersection_collection(input_basename, output_filename);

    return 0;
}
