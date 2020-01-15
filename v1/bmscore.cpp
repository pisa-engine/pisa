#include <optional>
#include <string>
#include <thread>

#include <CLI/CLI.hpp>

#include "app.hpp"
#include "v1/score_index.hpp"

using pisa::App;
using pisa::v1::BlockType;
using pisa::v1::FixedBlock;
using pisa::v1::VariableBlock;
namespace arg = pisa::arg;

int main(int argc, char** argv)
{
    std::optional<std::string> yml{};
    std::size_t block_size = 128;
    std::size_t threads = std::thread::hardware_concurrency();
    std::optional<float> lambda{};

    App<arg::Index, arg::Threads> app{"Constructs block-max score lists for v1 index."};
    app.add_option("--block-size", block_size, "The size of a block for max scores", true);
    app.add_option("--variable-blocks", lambda, "The size of a block for max scores", false);
    CLI11_PARSE(app, argc, argv);

    auto block_type = [&]() -> BlockType {
        if (lambda) {
            return VariableBlock{*lambda};
        }
        return FixedBlock{block_size};
    }();

    pisa::v1::bm_score_index(app.index_metadata(), block_type, app.threads());
    return 0;
}
