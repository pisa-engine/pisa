#include <optional>
#include <string>
#include <thread>

#include <CLI/CLI.hpp>

#include "v1/index_metadata.hpp"
#include "v1/score_index.hpp"

int main(int argc, char** argv)
{
    std::optional<std::string> yml{};
    int bytes_per_score = 1;
    std::size_t threads = std::thread::hardware_concurrency();

    CLI::App app{"Scores v1 index."};
    app.add_option("-i,--index",
                   yml,
                   "Path of .yml file of an index "
                   "(if not provided, it will be looked for in the current directory)",
                   false);
    app.add_option("-j,--threads", threads, "Number of threads");
    // TODO(michal): enable
    // app.add_option(
    //    "-b,--bytes-per-score", yml, "Quantize computed scores to this many bytes", true);
    CLI11_PARSE(app, argc, argv);
    pisa::v1::score_index(pisa::v1::resolve_yml(yml), threads);
    return 0;
}
