#include <fstream>
#include <iostream>
#include <optional>

#include <CLI/CLI.hpp>
#include <range/v3/view/filter.hpp>
#include <range/v3/view/transform.hpp>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/spdlog.h>

#include "app.hpp"
#include "io.hpp"
#include "query/queries.hpp"
#include "timer.hpp"
#include "topk_queue.hpp"
#include "v1/blocked_cursor.hpp"
#include "v1/daat_or.hpp"
#include "v1/index_metadata.hpp"
#include "v1/query.hpp"
#include "v1/raw_cursor.hpp"
#include "v1/scorer/bm25.hpp"
#include "v1/scorer/runner.hpp"
#include "v1/types.hpp"

using pisa::resolve_query_parser;
using pisa::v1::BlockedReader;
using pisa::v1::index_runner;
using pisa::v1::IndexMetadata;
using pisa::v1::Query;
using pisa::v1::RawReader;
using pisa::v1::resolve_yml;
using pisa::v1::VoidScorer;

template <typename Index, typename Scorer>
void calculate_thresholds(Index&& index,
                          Scorer&& scorer,
                          std::vector<Query>& queries,
                          std::ostream& os)
{
    for (auto&& query : queries) {
        auto results = pisa::v1::daat_or(
            query, index, ::pisa::topk_queue(query.k()), std::forward<Scorer>(scorer));
        results.finalize();
        float threshold = 0.0;
        if (not results.topk().empty()) {
            threshold = results.topk().back().first;
        }
        query.threshold(threshold);
        os << query.to_json() << '\n';
    }
}

int main(int argc, char** argv)
{
    spdlog::drop("");
    spdlog::set_default_logger(spdlog::stderr_color_mt(""));

    bool in_place = false;
    pisa::QueryApp app("Calculates thresholds for a v1 index.");
    app.add_flag("--in-place", in_place, "Edit the input file");
    CLI11_PARSE(app, argc, argv);

    if (in_place && not app.query_file()) {
        spdlog::error("Cannot edit in place when no query file passed");
        std::exit(1);
    }

    try {
        auto meta = app.index_metadata();
        auto queries = app.queries(meta);

        if (app.use_quantized()) {
            auto run = scored_index_runner(meta,
                                           RawReader<std::uint32_t>{},
                                           RawReader<std::uint8_t>{},
                                           BlockedReader<::pisa::simdbp_block, true>{},
                                           BlockedReader<::pisa::simdbp_block, false>{});
            run([&](auto&& index) {
                if (in_place) {
                    std::ofstream os(app.query_file().value());
                    calculate_thresholds(index, VoidScorer{}, queries, os);
                } else {
                    calculate_thresholds(index, VoidScorer{}, queries, std::cout);
                }
            });
        } else {
            auto run = index_runner(meta,
                                    RawReader<std::uint32_t>{},
                                    BlockedReader<::pisa::simdbp_block, true>{},
                                    BlockedReader<::pisa::simdbp_block, false>{});
            run([&](auto&& index) {
                auto with_scorer = scorer_runner(index, make_bm25(index));
                with_scorer("bm25", [&](auto scorer) {
                    if (in_place) {
                        std::ofstream os(app.query_file().value());
                        calculate_thresholds(index, scorer, queries, os);
                    } else {
                        calculate_thresholds(index, scorer, queries, std::cout);
                    }
                });
            });
        }
    } catch (std::exception const& error) {
        spdlog::error("{}", error.what());
    }
    return 0;
}
