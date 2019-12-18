#include <fstream>
#include <iostream>
#include <optional>

#include <CLI/CLI.hpp>
#include <range/v3/view/filter.hpp>
#include <range/v3/view/transform.hpp>
#include <spdlog/spdlog.h>

#include "app.hpp"
#include "io.hpp"
#include "query/queries.hpp"
#include "timer.hpp"
#include "topk_queue.hpp"
#include "v1/blocked_cursor.hpp"
#include "v1/daat_or.hpp"
#include "v1/index_metadata.hpp"
#include "v1/intersection.hpp"
#include "v1/maxscore.hpp"
#include "v1/query.hpp"
#include "v1/raw_cursor.hpp"
#include "v1/scorer/bm25.hpp"
#include "v1/scorer/runner.hpp"
#include "v1/types.hpp"
#include "v1/union_lookup.hpp"

using pisa::resolve_query_parser;
using pisa::TermProcessor;
using pisa::v1::BlockedReader;
using pisa::v1::daat_or;
using pisa::v1::index_runner;
using pisa::v1::IndexMetadata;
using pisa::v1::Query;
using pisa::v1::RawReader;
using pisa::v1::resolve_yml;
using pisa::v1::VoidScorer;

namespace arg = pisa::arg;

int main(int argc, char** argv)
{
    pisa::App<arg::Index, arg::Query<arg::QueryMode::Unranked>> app(
        "Filters out empty queries against a v1 index.");
    CLI11_PARSE(app, argc, argv);

    auto meta = app.index_metadata();
    auto queries = app.queries(meta);
    for (auto&& query : queries) {
        if (query.term_ids()) {
            std::cout << query.to_json() << '\n';
        }
    }
    return 0;
}
