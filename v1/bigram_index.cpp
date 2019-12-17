#include <chrono>
#include <fstream>
#include <iostream>
#include <optional>

#include <CLI/CLI.hpp>
#include <spdlog/spdlog.h>

#include "app.hpp"
#include "io.hpp"
#include "query/queries.hpp"
#include "timer.hpp"
#include "topk_queue.hpp"
#include "v1/blocked_cursor.hpp"
#include "v1/cursor_intersection.hpp"
#include "v1/index_builder.hpp"
#include "v1/index_metadata.hpp"
#include "v1/progress_status.hpp"
#include "v1/query.hpp"
#include "v1/raw_cursor.hpp"
#include "v1/scorer/bm25.hpp"
#include "v1/scorer/runner.hpp"
#include "v1/types.hpp"

using pisa::App;
using pisa::v1::build_bigram_index;
using pisa::v1::collect_unique_bigrams;
using pisa::v1::DefaultProgress;
using pisa::v1::DocId;
using pisa::v1::Frequency;
using pisa::v1::IndexMetadata;
using pisa::v1::ProgressStatus;
using pisa::v1::Query;
using pisa::v1::resolve_yml;
using pisa::v1::TermId;

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
