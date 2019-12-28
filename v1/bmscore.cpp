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
    std::size_t block_size;
    std::size_t threads = std::thread::hardware_concurrency();

    App<arg::Index, arg::Threads> app{"Constructs block-max score lists for v1 index."};
    app.add_option("--block-size", block_size, "The size of a block for max scores", false)
        ->required();
    CLI11_PARSE(app, argc, argv);
    pisa::v1::bm_score_index(app.index_metadata(), block_size, app.threads());
    return 0;
}
