#include <chrono>
#include <fstream>
#include <iostream>
#include <optional>

#include <CLI/CLI.hpp>
#include <spdlog/spdlog.h>

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

int main(int argc, char** argv)
{
    std::optional<std::string> yml{};
    std::optional<std::string> query_file{};
    std::optional<std::string> terms_file{};

    CLI::App app{"Creates a v1 bigram index."};
    app.add_option("-i,--index",
                   yml,
                   "Path of .yml file of an index "
                   "(if not provided, it will be looked for in the current directory)",
                   false);
    app.add_option("-q,--query", query_file, "Path to file with queries", false);
    app.add_option("--terms", terms_file, "Overrides document lexicon from .yml (if defined).");
    CLI11_PARSE(app, argc, argv);

    auto resolved_yml = resolve_yml(yml);
    auto meta = IndexMetadata::from_file(resolved_yml);
    auto stemmer = meta.stemmer ? std::make_optional(*meta.stemmer) : std::optional<std::string>{};
    if (meta.term_lexicon) {
        terms_file = meta.term_lexicon.value();
    }

    spdlog::info("Collecting queries...");
    auto queries = [&]() {
        std::vector<::pisa::Query> queries;
        auto parse_query = resolve_query_parser(queries, terms_file, std::nullopt, stemmer);
        if (query_file) {
            std::ifstream is(*query_file);
            pisa::io::for_each_line(is, parse_query);
        } else {
            pisa::io::for_each_line(std::cin, parse_query);
        }
        std::vector<Query> v1_queries;
        v1_queries.reserve(queries.size());
        for (auto query : queries) {
            if (not query.terms.empty()) {
                v1_queries.push_back(Query{.terms = query.terms,
                                           .list_selection = {},
                                           .threshold = {},
                                           .id =
                                               [&]() {
                                                   if (query.id) {
                                                       return tl::make_optional(*query.id);
                                                   }
                                                   return tl::optional<std::string>{};
                                               }(),
                                           .k = 10});
            }
        }
        return v1_queries;
    }();

    spdlog::info("Collected {} queries", queries.size());
    spdlog::info("Collecting bigrams...");
    ProgressStatus status(queries.size(), DefaultProgress{}, std::chrono::milliseconds(1000));
    auto bigrams = collect_unique_bigrams(queries, [&]() { status += 1; });
    spdlog::info("Collected {} bigrams", bigrams.size());
    build_bigram_index(resolved_yml, bigrams);
    return 0;
}
