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
#include "util/do_not_optimize_away.hpp"
#include "v1/blocked_cursor.hpp"
#include "v1/daat_or.hpp"
#include "v1/index_metadata.hpp"
#include "v1/inspect_query.hpp"
#include "v1/intersection.hpp"
#include "v1/maxscore.hpp"
#include "v1/query.hpp"
#include "v1/raw_cursor.hpp"
#include "v1/scorer/bm25.hpp"
#include "v1/scorer/runner.hpp"
#include "v1/types.hpp"
#include "v1/union_lookup.hpp"

using pisa::resolve_query_parser;
using pisa::v1::BlockedReader;
using pisa::v1::daat_or;
using pisa::v1::DaatOrInspector;
using pisa::v1::index_runner;
using pisa::v1::IndexMetadata;
using pisa::v1::ListSelection;
using pisa::v1::lookup_union;
using pisa::v1::LookupUnionInspector;
using pisa::v1::maxscore_union_lookup;
using pisa::v1::MaxscoreInspector;
using pisa::v1::MaxscoreUnionLookupInspect;
using pisa::v1::Query;
using pisa::v1::QueryInspector;
using pisa::v1::RawReader;
using pisa::v1::resolve_yml;
using pisa::v1::unigram_union_lookup;
using pisa::v1::UnigramUnionLookupInspect;
using pisa::v1::union_lookup;
using pisa::v1::UnionLookupInspect;
using pisa::v1::VoidScorer;

using RetrievalAlgorithm = std::function<::pisa::topk_queue(pisa::v1::Query, ::pisa::topk_queue)>;

template <typename Index, typename Scorer>
auto resolve_algorithm(std::string const& name, Index const& index, Scorer&& scorer)
    -> RetrievalAlgorithm
{
    if (name == "daat_or") {
        return RetrievalAlgorithm([&](pisa::v1::Query const& query, ::pisa::topk_queue topk) {
            return pisa::v1::daat_or(query, index, std::move(topk), std::forward<Scorer>(scorer));
        });
    }
    if (name == "maxscore") {
        return RetrievalAlgorithm([&](pisa::v1::Query const& query, ::pisa::topk_queue topk) {
            if (query.threshold()) {
                topk.set_threshold(query.get_threshold());
            }
            return pisa::v1::maxscore(query, index, std::move(topk), std::forward<Scorer>(scorer));
        });
    }
    if (name == "maxscore-union-lookup") {
        return RetrievalAlgorithm([&](pisa::v1::Query const& query, ::pisa::topk_queue topk) {
            return pisa::v1::maxscore_union_lookup(
                query, index, std::move(topk), std::forward<Scorer>(scorer));
        });
    }
    if (name == "unigram-union-lookup") {
        return RetrievalAlgorithm([&](pisa::v1::Query const& query, ::pisa::topk_queue topk) {
            return pisa::v1::unigram_union_lookup(
                query, index, std::move(topk), std::forward<Scorer>(scorer));
        });
    }
    if (name == "union-lookup") {
        return RetrievalAlgorithm([&](pisa::v1::Query const& query, ::pisa::topk_queue topk) {
            if (query.selections()->bigrams.empty()) {
                return pisa::v1::unigram_union_lookup(
                    query, index, std::move(topk), std::forward<Scorer>(scorer));
            }
            if (query.get_term_ids().size() > 8) {
                return pisa::v1::maxscore(
                    query, index, std::move(topk), std::forward<Scorer>(scorer));
            }
            return pisa::v1::union_lookup(
                query, index, std::move(topk), std::forward<Scorer>(scorer));
        });
    }
    if (name == "lookup-union") {
        return RetrievalAlgorithm([&](pisa::v1::Query const& query, ::pisa::topk_queue topk) {
            if (query.selections()->bigrams.empty()) {
                return pisa::v1::unigram_union_lookup(
                    query, index, std::move(topk), std::forward<Scorer>(scorer));
            }
            return pisa::v1::lookup_union(
                query, index, std::move(topk), std::forward<Scorer>(scorer));
        });
    }
    spdlog::error("Unknown algorithm: {}", name);
    std::exit(1);
}

template <typename Index, typename Scorer>
auto resolve_inspect(std::string const& name, Index const& index, Scorer&& scorer) -> QueryInspector
{
    if (name == "daat_or") {
        return QueryInspector(DaatOrInspector(index, std::forward<Scorer>(scorer)));
    }
    if (name == "maxscore") {
        return QueryInspector(MaxscoreInspector(index, std::forward<Scorer>(scorer)));
    }
    if (name == "maxscore-union-lookup") {
        return QueryInspector(
            MaxscoreUnionLookupInspect<Index, std::decay_t<Scorer>>(index, scorer));
    }
    if (name == "unigram-union-lookup") {
        return QueryInspector(
            UnigramUnionLookupInspect<Index, std::decay_t<Scorer>>(index, scorer));
    }
    if (name == "union-lookup") {
        return QueryInspector(UnionLookupInspect<Index, std::decay_t<Scorer>>(index, scorer));
    }
    if (name == "lookup-union") {
        return QueryInspector(LookupUnionInspector<Index, std::decay_t<Scorer>>(index, scorer));
    }
    spdlog::error("Unknown algorithm: {}", name);
    std::exit(1);
}

void evaluate(std::vector<Query> const& queries,
              pisa::Payload_Vector<> const& docmap,
              RetrievalAlgorithm const& retrieve)
{
    auto query_idx = 0;
    for (auto const& query : queries) {
        auto que = retrieve(query, pisa::topk_queue(query.k()));
        que.finalize();
        auto rank = 0;
        for (auto result : que.topk()) {
            std::cout << fmt::format("{}\t{}\t{}\t{}\t{}\t{}\n",
                                     query.id().value_or(std::to_string(query_idx)),
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

void benchmark(std::vector<Query> const& queries, RetrievalAlgorithm retrieve)

{
    std::vector<double> times(queries.size(), std::numeric_limits<double>::max());
    for (auto run = 0; run < 5; run += 1) {
        for (auto query = 0; query < queries.size(); query += 1) {
            auto usecs = ::pisa::run_with_timer<std::chrono::microseconds>([&]() {
                auto que = retrieve(queries[query], pisa::topk_queue(queries[query].k()));
                que.finalize();
                do_not_optimize_away(que);
            });
            times[query] = std::min(times[query], static_cast<double>(usecs.count()));
        }
    }
    for (auto time : times) {
        std::cout << time << '\n';
    }
    std::sort(times.begin(), times.end());
    double avg = std::accumulate(times.begin(), times.end(), double()) / times.size();
    double q50 = times[times.size() / 2];
    double q90 = times[90 * times.size() / 100];
    double q95 = times[95 * times.size() / 100];
    spdlog::info("Mean: {} us", avg);
    spdlog::info("50% quantile: {} us", q50);
    spdlog::info("90% quantile: {} us", q90);
    spdlog::info("95% quantile: {} us", q95);
}

void inspect_queries(std::vector<Query> const& queries, QueryInspector inspect)
{
    for (auto query = 0; query < queries.size(); query += 1) {
        inspect(queries[query]);
    }
    std::move(inspect).summarize();
}

int main(int argc, char** argv)
{
    spdlog::drop("");
    spdlog::set_default_logger(spdlog::stderr_color_mt(""));

    std::string algorithm = "daat_or";
    bool inspect = false;

    pisa::QueryApp app("Queries a v1 index.");
    app.add_option("--algorithm", algorithm, "Query retrieval algorithm.", true);
    app.add_flag("--inspect", inspect, "Analyze query execution and stats");
    CLI11_PARSE(app, argc, argv);

    try {
        auto meta = app.index_metadata();
        auto queries = app.queries(meta);

        if (not meta.document_lexicon) {
            spdlog::error("Document lexicon not defined");
            std::exit(1);
        }
        auto source = std::make_shared<mio::mmap_source>(meta.document_lexicon.value().c_str());
        auto docmap = pisa::Payload_Vector<>::from(*source);

        if (app.use_quantized()) {
            auto run = scored_index_runner(meta,
                                           RawReader<std::uint32_t>{},
                                           RawReader<std::uint8_t>{},
                                           BlockedReader<::pisa::simdbp_block, true>{},
                                           BlockedReader<::pisa::simdbp_block, false>{});
            run([&](auto&& index) {
                if (app.is_benchmark()) {
                    benchmark(queries, resolve_algorithm(algorithm, index, VoidScorer{}));
                } else if (inspect) {
                    inspect_queries(queries, resolve_inspect(algorithm, index, VoidScorer{}));
                } else {
                    evaluate(queries, docmap, resolve_algorithm(algorithm, index, VoidScorer{}));
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
                    if (app.is_benchmark()) {
                        benchmark(queries, resolve_algorithm(algorithm, index, scorer));
                    } else if (inspect) {
                        inspect_queries(queries, resolve_inspect(algorithm, index, scorer));
                    } else {
                        evaluate(queries, docmap, resolve_algorithm(algorithm, index, scorer));
                    }
                });
            });
        }
    } catch (std::exception const& error) {
        spdlog::error("{}", error.what());
    }
    return 0;
}
