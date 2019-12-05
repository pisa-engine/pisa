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
void calculate_thresholds(Index&& index, Scorer&& scorer, std::vector<Query> const& queries, int k)
{
    for (auto const& query : queries) {
        auto results =
            pisa::v1::daat_or(query, index, ::pisa::topk_queue(k), std::forward<Scorer>(scorer));
        results.finalize();
        float threshold = 0.0;
        if (not results.topk().empty()) {
            threshold = results.topk().back().first;
        }
        std::cout << threshold << '\n';
    }
}

int main(int argc, char** argv)
{
    spdlog::drop("");
    spdlog::set_default_logger(spdlog::stderr_color_mt(""));
    pisa::QueryApp app("Calculates thresholds for a v1 index.");
    CLI11_PARSE(app, argc, argv);

    try {
        auto meta = IndexMetadata::from_file(resolve_yml(app.yml));
        auto stemmer =
            meta.stemmer ? std::make_optional(*meta.stemmer) : std::optional<std::string>{};
        if (meta.term_lexicon) {
            app.terms_file = meta.term_lexicon.value();
        }

        auto queries = [&]() {
            std::vector<::pisa::Query> queries;
            auto parse_query = resolve_query_parser(queries, app.terms_file, std::nullopt, stemmer);
            if (app.query_file) {
                std::ifstream is(*app.query_file);
                pisa::io::for_each_line(is, parse_query);
            } else {
                pisa::io::for_each_line(std::cin, parse_query);
            }
            std::vector<Query> v1_queries(queries.size());
            std::transform(queries.begin(), queries.end(), v1_queries.begin(), [&](auto&& parsed) {
                Query query(parsed.terms);
                if (parsed.id) {
                    query.id(*parsed.id);
                }
                query.k(app.k);
                return query;
            });
            return v1_queries;
        }();

        if (app.precomputed) {
            auto run = scored_index_runner(meta,
                                           RawReader<std::uint32_t>{},
                                           RawReader<std::uint8_t>{},
                                           BlockedReader<::pisa::simdbp_block, true>{},
                                           BlockedReader<::pisa::simdbp_block, false>{});
            run([&](auto&& index) { calculate_thresholds(index, VoidScorer{}, queries, app.k); });
        } else {
            auto run = index_runner(meta,
                                    RawReader<std::uint32_t>{},
                                    BlockedReader<::pisa::simdbp_block, true>{},
                                    BlockedReader<::pisa::simdbp_block, false>{});
            run([&](auto&& index) {
                auto with_scorer = scorer_runner(index, make_bm25(index));
                with_scorer("bm25", [&](auto scorer) {
                    calculate_thresholds(index, scorer, queries, app.k);
                });
            });
        }
    } catch (std::exception const& error) {
        spdlog::error("{}", error.what());
    }
    return 0;
}
