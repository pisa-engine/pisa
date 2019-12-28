#include <iostream>
#include <optional>

#include <CLI/CLI.hpp>
#include <spdlog/spdlog.h>

#include "app.hpp"
#include "v1/index_builder.hpp"
#include "v1/progress_status.hpp"
#include "v1/types.hpp"

using pisa::App;
using pisa::v1::build_bigram_index;
using pisa::v1::collect_unique_bigrams;
using pisa::v1::DefaultProgress;
using pisa::v1::ProgressStatus;

namespace arg = pisa::arg;

int main(int argc, char** argv)
{
    std::optional<std::string> terms_file{};

    App<arg::Index, arg::Query<arg::QueryMode::Unranked>> app{"Creates a v1 bigram index."};
    CLI11_PARSE(app, argc, argv);

    auto meta = app.index_metadata();

    spdlog::info("Collecting queries...");
    auto queries = app.queries(meta);

    spdlog::info("Collected {} queries", queries.size());
    spdlog::info("Collecting bigrams...");
    ProgressStatus status(queries.size(), DefaultProgress{}, std::chrono::milliseconds(1000));
    auto bigrams = collect_unique_bigrams(queries, [&]() { status += 1; });
    spdlog::info("Collected {} bigrams", bigrams.size());
    build_bigram_index(meta, bigrams);
    return 0;
}
