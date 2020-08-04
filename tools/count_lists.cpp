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
    size_t min_len = 0;
    size_t max_len = std::numeric_limits<size_t>::max();

    CLI::App app{"Counts all postings in the index."};
    app.add_option("-c,--collection", input_basename, "Collection basename")->required();
    app.add_option("-m,--min", min_len, "Min length");
    app.add_option("-M,--max", max_len, "Max length");
    CLI11_PARSE(app, argc, argv);

    binary_collection coll((input_basename + ".docs").c_str());
    auto firstseq = *coll.begin();
    if (firstseq.size() != 1) {
        throw std::invalid_argument("First sequence should only contain number of documents");
    }

    std::size_t count = 0;
    for (auto&& p: coll) {
        if(p.size() >= min_len && p.size() <= max_len)
            count += 1;
    }

    std::cout << count << '\n';
    return 0;
}

