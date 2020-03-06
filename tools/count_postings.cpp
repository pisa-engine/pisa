#include <iostream>

#include <CLI/CLI.hpp>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/spdlog.h>

#include "binary_collection.hpp"

using namespace pisa;

int main(int argc, char** argv)
{
    spdlog::drop("");
    spdlog::set_default_logger(spdlog::stderr_color_mt(""));
    std::string input_basename;

    CLI::App app{"Counts all postings in the index."};
    app.add_option("-c,--collection", input_basename, "Collection basename")->required();
    CLI11_PARSE(app, argc, argv);

    binary_collection coll((input_basename + ".docs").c_str());
    auto firstseq = *coll.begin();
    if (firstseq.size() != 1) {
        throw std::invalid_argument("First sequence should only contain number of documents");
    }

    std::size_t count = -1;  // Takes care of the first 'fake' sequence
    for (auto&& p: coll) {
        count += p.size();
    }

    std::cout << count << '\n';
    return 0;
}
