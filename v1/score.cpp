#include <cstdint>
#include <optional>
#include <string>
#include <thread>

#include <CLI/CLI.hpp>

#include "app.hpp"
#include "v1/score_index.hpp"

using pisa::App;
namespace arg = pisa::arg;

int main(int argc, char** argv)
{
    std::optional<std::string> yml{};
    int bytes_per_score = 1;
    std::size_t threads = std::thread::hardware_concurrency();

    App<arg::Index, arg::Threads> app{"Scores v1 index."};
    // TODO(michal): enable
    // app.add_option(
    //    "-b,--bytes-per-score", yml, "Quantize computed scores to this many bytes", true);
    CLI11_PARSE(app, argc, argv);
    pisa::v1::score_index(app.index_metadata(), app.threads());
    return 0;
}
