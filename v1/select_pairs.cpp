#include <iostream>
#include <optional>

#include <CLI/CLI.hpp>
#include <fmt/ostream.h>
#include <spdlog/spdlog.h>

#include "app.hpp"
#include "v1/default_index_runner.hpp"
#include "v1/index_builder.hpp"
#include "v1/index_metadata.hpp"
#include "v1/progress_status.hpp"
#include "v1/types.hpp"

using pisa::App;
using pisa::v1::bigram_gain;
using pisa::v1::index_runner;

namespace arg = pisa::arg;

int main(int argc, char** argv)
{
    std::optional<std::string> terms_file{};
    //std::size_t num_pairs_to_select;

    App<arg::Index, arg::Query<arg::QueryMode::Unranked>> app{"Creates a v1 bigram index."};
    // app.add_option("--count", num_pairs_to_select, "Number of pairs to select")->required();
    CLI11_PARSE(app, argc, argv);

    auto meta = app.index_metadata();

    auto run = index_runner(meta);
    run([&](auto&& index) {
        for (auto&& query : app.query_range(meta)) {
            auto term_ids = query.get_term_ids();
            std::cout
                << fmt::format("{}\t{}\t{}\n", term_ids[0], term_ids[1], bigram_gain(index, query));
        }
    });
    return 0;
}
