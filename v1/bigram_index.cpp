#include <iostream>
#include <optional>

#include <CLI/CLI.hpp>
#include <spdlog/spdlog.h>
#include <tl/optional.hpp>

#include "app.hpp"
#include "v1/index_builder.hpp"
#include "v1/progress_status.hpp"
#include "v1/types.hpp"

using pisa::App;
using pisa::v1::build_pair_index;
using pisa::v1::collect_unique_bigrams;
using pisa::v1::DefaultProgressCallback;
using pisa::v1::ProgressStatus;

namespace arg = pisa::arg;

int main(int argc, char** argv)
{
    tl::optional<std::string> clone_path{};

    App<arg::Index, arg::Query<arg::QueryMode::Unranked>, arg::Threads> app{
        "Creates a v1 bigram index."};
    app.add_option("--clone", clone_path, "Instead", false);
    CLI11_PARSE(app, argc, argv);

    auto meta = app.index_metadata();

    spdlog::info("Collecting queries...");
    auto queries = app.queries(meta);

    spdlog::info("Collected {} queries", queries.size());
    spdlog::info("Collecting bigrams...");
    ProgressStatus status(
        queries.size(), DefaultProgressCallback{}, std::chrono::milliseconds(1000));
    auto bigrams = collect_unique_bigrams(queries, [&]() { status += 1; });
    status.close();
    spdlog::info("Collected {} bigrams", bigrams.size());
    build_pair_index(meta, bigrams, clone_path, app.threads());
    return 0;
}
