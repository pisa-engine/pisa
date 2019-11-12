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
#include "v1/index_metadata.hpp"
#include "v1/query.hpp"
#include "v1/raw_cursor.hpp"
#include "v1/scorer/bm25.hpp"
#include "v1/scorer/runner.hpp"
#include "v1/types.hpp"

using pisa::Query;
using pisa::resolve_query_parser;
using pisa::v1::BlockedReader;
using pisa::v1::daat_or;
using pisa::v1::index_runner;
using pisa::v1::IndexMetadata;
using pisa::v1::RawReader;
using pisa::v1::resolve_ini;
using pisa::v1::VoidScorer;

template <typename Index, typename Scorer>
void evaluate(std::vector<pisa::Query> const &queries,
              Index &&index,
              Scorer &&scorer,
              int k,
              pisa::Payload_Vector<> const &docmap)
{
    auto query_idx = 0;
    for (auto const &query : queries) {
        auto que = daat_or(pisa::v1::Query{query.terms}, index, pisa::topk_queue(k), scorer);
        que.finalize();
        auto rank = 0;
        for (auto result : que.topk()) {
            std::cout << fmt::format("{}\t{}\t{}\t{}\t{}\t{}\n",
                                     query.id.value_or(std::to_string(query_idx)),
                                     "Q0",
                                     docmap[result.second],
                                     rank,
                                     result.first,
                                     "R0");
            rank += 1;
        }
        query_idx += 1;
    }
}

template <typename Index, typename Scorer>
void benchmark(std::vector<pisa::Query> const &queries, Index &&index, Scorer &&scorer, int k)

{
    std::vector<double> times(queries.size(), std::numeric_limits<double>::max());
    for (auto run = 0; run < 5; run += 1) {
        for (auto query = 0; query < queries.size(); query += 1) {
            auto usecs = ::pisa::run_with_timer<std::chrono::microseconds>([&]() {
                auto que = daat_or(
                    pisa::v1::Query{queries[query].terms}, index, pisa::topk_queue(k), scorer);
                que.finalize();
                do_not_optimize_away(que);
            });
            times[query] = std::min(times[query], static_cast<double>(usecs.count()));
        }
    }
    std::sort(times.begin(), times.end());
    double avg = std::accumulate(times.begin(), times.end(), double()) / times.size();
    double q50 = times[times.size() / 2];
    double q90 = times[90 * times.size() / 100];
    double q95 = times[95 * times.size() / 100];
    spdlog::info("Mean: {}", avg);
    spdlog::info("50% quantile: {}", q50);
    spdlog::info("90% quantile: {}", q90);
    spdlog::info("95% quantile: {}", q95);
}

int main(int argc, char **argv)
{
    std::optional<std::string> ini{};
    std::optional<std::string> query_file{};
    std::optional<std::string> terms_file{};
    std::optional<std::string> documents_file{};
    int k = 1'000;
    bool is_benchmark = false;
    bool precomputed = false;

    CLI::App app{"Queries a v1 index."};
    app.add_option("-i,--index",
                   ini,
                   "Path of .ini file of an index "
                   "(if not provided, it will be looked for in the current directory)",
                   false);
    app.add_option("-q,--query", query_file, "Path to file with queries", false);
    app.add_option("-k", k, "The number of top results to return", true);
    app.add_option("--terms", terms_file, "Overrides document lexicon from .ini (if defined).");
    app.add_option("--documents",
                   documents_file,
                   "Overrides document lexicon from .ini (if defined). Required otherwise.");
    app.add_flag("--benchmark", is_benchmark, "Run benchmark");
    app.add_flag("--precomputed", precomputed, "Use precomputed scores");
    CLI11_PARSE(app, argc, argv);

    auto meta = IndexMetadata::from_file(resolve_ini(ini));
    auto stemmer = meta.stemmer ? std::make_optional(*meta.stemmer) : std::optional<std::string>{};
    if (meta.term_lexicon) {
        terms_file = meta.term_lexicon.value();
    }
    if (meta.document_lexicon) {
        documents_file = meta.document_lexicon.value();
    }

    std::vector<pisa::Query> queries;
    auto parse_query = resolve_query_parser(queries, terms_file, std::nullopt, stemmer);
    if (query_file) {
        std::ifstream is(*query_file);
        pisa::io::for_each_line(is, parse_query);
    } else {
        pisa::io::for_each_line(std::cin, parse_query);
    }

    if (not documents_file) {
        spdlog::error("Document lexicon not defined");
        std::exit(1);
    }
    auto source = std::make_shared<mio::mmap_source>(documents_file.value().c_str());
    auto docmap = pisa::Payload_Vector<>::from(*source);

    if (precomputed) {
        auto run = scored_index_runner(meta,
                                       RawReader<std::uint32_t>{},
                                       RawReader<std::uint8_t>{},
                                       BlockedReader<::pisa::simdbp_block, true>{},
                                       BlockedReader<::pisa::simdbp_block, false>{});
        run([&](auto &&index) {
            if (is_benchmark) {
                benchmark(queries, index, VoidScorer{}, k);
            } else {
                evaluate(queries, index, VoidScorer{}, k, docmap);
            }
        });
    } else {
        auto run = index_runner(meta,
                                RawReader<std::uint32_t>{},
                                BlockedReader<::pisa::simdbp_block, true>{},
                                BlockedReader<::pisa::simdbp_block, false>{});
        run([&](auto &&index) {
            auto with_scorer = scorer_runner(index, make_bm25(index));
            with_scorer("bm25", [&](auto scorer) {
                if (is_benchmark) {
                    benchmark(queries, index, scorer, k);
                } else {
                    evaluate(queries, index, scorer, k, docmap);
                }
            });
        });
    }
    return 0;
}
