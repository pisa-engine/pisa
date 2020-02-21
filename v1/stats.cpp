#include <iostream>

#include <CLI/CLI.hpp>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/spdlog.h>

#include "app.hpp"
#include "v1/default_index_runner.hpp"

namespace arg = pisa::arg;

int main(int argc, char** argv)
{
    spdlog::drop("");
    spdlog::set_default_logger(spdlog::stderr_color_mt(""));

    pisa::App<arg::Index> app("Simply counts all postings in the index");
    CLI11_PARSE(app, argc, argv);

    auto meta = app.index_metadata();
    std::size_t count{0};
    pisa::v1::index_runner(meta)([&](auto&& index) {
        std::cout << fmt::format("#terms: {}\n", index.num_terms());
        std::cout << fmt::format("#documents: {}\n", index.num_documents());
        std::cout << fmt::format("#pairs: {}\n", index.num_pairs());
        std::cout << fmt::format("avg. document length: {}\n", index.avg_document_length());
    });
    return 0;
}
