#include <fstream>
#include <iostream>
#include <optional>

#include <CLI/CLI.hpp>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/spdlog.h>

#include "app.hpp"
#include "query/queries.hpp"
#include "timer.hpp"
#include "topk_queue.hpp"
#include "util/do_not_optimize_away.hpp"
#include "v1/blocked_cursor.hpp"
#include "v1/daat_or.hpp"
#include "v1/default_index_runner.hpp"
#include "v1/index_metadata.hpp"
#include "v1/inspect_query.hpp"
#include "v1/maxscore.hpp"
#include "v1/maxscore_union_lookup.hpp"
#include "v1/query.hpp"
#include "v1/raw_cursor.hpp"
#include "v1/scorer/bm25.hpp"
#include "v1/scorer/runner.hpp"
#include "v1/types.hpp"
#include "v1/unigram_union_lookup.hpp"
#include "v1/union_lookup.hpp"
#include "v1/wand.hpp"

using pisa::v1::daat_or;
using pisa::v1::DocumentBlockedReader;
using pisa::v1::index_runner;
using pisa::v1::InspectDaatOr;
using pisa::v1::InspectLookupUnion;
using pisa::v1::InspectLookupUnionEaat;
using pisa::v1::InspectMaxScore;
using pisa::v1::InspectMaxScoreUnionLookup;
using pisa::v1::InspectUnigramUnionLookup;
using pisa::v1::InspectUnionLookup;
using pisa::v1::InspectUnionLookupPlus;
using pisa::v1::lookup_union;
using pisa::v1::maxscore_union_lookup;
using pisa::v1::PayloadBlockedReader;
using pisa::v1::Query;
using pisa::v1::QueryInspector;
using pisa::v1::RawReader;
using pisa::v1::unigram_union_lookup;
using pisa::v1::union_lookup;
using pisa::v1::union_lookup_plus;
using pisa::v1::VoidScorer;
using pisa::v1::wand;

struct RetrievalAlgorithm {
    template <typename Fn, typename FallbackFn>
    explicit RetrievalAlgorithm(Fn fn, FallbackFn fallback, bool safe)
        : m_retrieve(std::move(fn)), m_fallback(std::move(fallback)), m_safe(safe)
    {
    }

    [[nodiscard]] auto operator()(pisa::v1::Query const& query, ::pisa::topk_queue topk) const
        -> ::pisa::topk_queue
    {
        topk = m_retrieve(query, topk);
        if (m_safe && not topk.full()) {
            spdlog::debug("Retrieved {} out of {} documents. Rerunning without threshold.",
                          topk.topk().size(),
                          topk.size());
            topk.clear();
            topk = m_fallback(query, topk);
        }
        return topk;
    }

   private:
    std::function<::pisa::topk_queue(pisa::v1::Query, ::pisa::topk_queue)> m_retrieve;
    std::function<::pisa::topk_queue(pisa::v1::Query, ::pisa::topk_queue)> m_fallback;
    bool m_safe;
};

template <typename Index, typename Scorer>
auto resolve_algorithm(std::string const& name, Index const& index, Scorer&& scorer, bool safe)
    -> RetrievalAlgorithm
{
    auto fallback = [&](pisa::v1::Query const& query, ::pisa::topk_queue topk) {
        topk.clear();
        return pisa::v1::maxscore(query, index, std::move(topk), std::forward<Scorer>(scorer));
    };
    if (name == "daat_or") {
        return RetrievalAlgorithm(
            [&](pisa::v1::Query const& query, ::pisa::topk_queue topk) {
                return pisa::v1::daat_or(
                    query, index, std::move(topk), std::forward<Scorer>(scorer));
            },
            fallback,
            safe);
    }
    if (name == "wand") {
        return RetrievalAlgorithm(
            [&](pisa::v1::Query const& query, ::pisa::topk_queue topk) {
                if (query.threshold()) {
                    topk.set_threshold(query.get_threshold());
                }
                return pisa::v1::wand(query, index, std::move(topk), std::forward<Scorer>(scorer));
            },
            fallback,
            safe);
    }
    if (name == "bmw") {
        return RetrievalAlgorithm(
            [&](pisa::v1::Query const& query, ::pisa::topk_queue topk) {
                if (query.threshold()) {
                    topk.set_threshold(query.get_threshold());
                }
                return pisa::v1::bmw(query, index, std::move(topk), std::forward<Scorer>(scorer));
            },
            fallback,
            safe);
    }
    if (name == "maxscore") {
        return RetrievalAlgorithm(
            [&](pisa::v1::Query const& query, ::pisa::topk_queue topk) {
                if (query.threshold()) {
                    topk.set_threshold(query.get_threshold());
                }
                return pisa::v1::maxscore(
                    query, index, std::move(topk), std::forward<Scorer>(scorer));
            },
            fallback,
            safe);
    }
    if (name == "maxscore-union-lookup") {
        return RetrievalAlgorithm(
            [&](pisa::v1::Query const& query, ::pisa::topk_queue topk) {
                return pisa::v1::maxscore_union_lookup(
                    query, index, std::move(topk), std::forward<Scorer>(scorer));
            },
            fallback,
            safe);
    }
    if (name == "unigram-union-lookup") {
        return RetrievalAlgorithm(
            [&](pisa::v1::Query const& query, ::pisa::topk_queue topk) {
                return pisa::v1::unigram_union_lookup(
                    query, index, std::move(topk), std::forward<Scorer>(scorer));
            },
            fallback,
            safe);
    }
    if (name == "union-lookup") {
        return RetrievalAlgorithm(
            [&](pisa::v1::Query const& query, ::pisa::topk_queue topk) {
                if (query.selections()->bigrams.empty()) {
                    return pisa::v1::unigram_union_lookup(
                        query, index, std::move(topk), std::forward<Scorer>(scorer));
                }
                if (query.get_term_ids().size() >= 8) {
                    return pisa::v1::maxscore(
                        query, index, std::move(topk), std::forward<Scorer>(scorer));
                }
                return pisa::v1::union_lookup(
                    query, index, std::move(topk), std::forward<Scorer>(scorer));
            },
            fallback,
            safe);
    }
    if (name == "union-lookup-plus") {
        return RetrievalAlgorithm(
            [&](pisa::v1::Query const& query, ::pisa::topk_queue topk) {
                if (query.selections()->bigrams.empty()) {
                    return pisa::v1::unigram_union_lookup(
                        query, index, std::move(topk), std::forward<Scorer>(scorer));
                }
                if (query.get_term_ids().size() > 8) {
                    return pisa::v1::maxscore(
                        query, index, std::move(topk), std::forward<Scorer>(scorer));
                }
                return pisa::v1::union_lookup_plus(
                    query, index, std::move(topk), std::forward<Scorer>(scorer));
            },
            fallback,
            safe);
    }
    if (name == "lookup-union") {
        return RetrievalAlgorithm(
            [&](pisa::v1::Query const& query, ::pisa::topk_queue topk) {
                if (query.selections()->bigrams.empty()) {
                    if (query.selections()->unigrams.empty()) {
                        return pisa::v1::maxscore(
                            query, index, std::move(topk), std::forward<Scorer>(scorer));
                    }
                    return pisa::v1::unigram_union_lookup(
                        query, index, std::move(topk), std::forward<Scorer>(scorer));
                }
                return pisa::v1::lookup_union(
                    query, index, std::move(topk), std::forward<Scorer>(scorer));
            },
            fallback,
            safe);
    }
    if (name == "lookup-union-eaat") {
        return RetrievalAlgorithm(
            [&](pisa::v1::Query const& query, ::pisa::topk_queue topk) {
                if (query.selections()->bigrams.empty()) {
                    if (query.selections()->unigrams.empty()) {
                        return pisa::v1::maxscore(
                            query, index, std::move(topk), std::forward<Scorer>(scorer));
                    }
                    return pisa::v1::unigram_union_lookup(
                        query, index, std::move(topk), std::forward<Scorer>(scorer));
                }
                return pisa::v1::lookup_union_eaat(
                    query, index, std::move(topk), std::forward<Scorer>(scorer));
            },
            fallback,
            safe);
    }
    spdlog::error("Unknown algorithm: {}", name);
    std::exit(1);
}

template <typename Index, typename Scorer>
auto resolve_inspect(std::string const& name, Index const& index, Scorer&& scorer) -> QueryInspector
{
    if (name == "daat_or") {
        return QueryInspector(InspectDaatOr(index, std::forward<Scorer>(scorer)));
    }
    if (name == "maxscore") {
        return QueryInspector(InspectMaxScore(index, std::forward<Scorer>(scorer)));
    }
    if (name == "maxscore-union-lookup") {
        return QueryInspector(
            InspectMaxScoreUnionLookup<Index, std::decay_t<Scorer>>(index, scorer));
    }
    if (name == "unigram-union-lookup") {
        return QueryInspector(
            InspectUnigramUnionLookup<Index, std::decay_t<Scorer>>(index, scorer));
    }
    if (name == "union-lookup") {
        return QueryInspector(InspectUnionLookup<Index, std::decay_t<Scorer>>(index, scorer));
    }
    if (name == "lookup-union") {
        return QueryInspector(InspectLookupUnion<Index, std::decay_t<Scorer>>(index, scorer));
    }
    if (name == "lookup-union-eaat") {
        return QueryInspector(InspectLookupUnionEaat<Index, std::decay_t<Scorer>>(index, scorer));
    }
    if (name == "union-lookup-plus") {
        return QueryInspector(InspectUnionLookupPlus<Index, std::decay_t<Scorer>>(index, scorer));
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
    inspect.header(std::cout);
    std::cout << '\n';
    for (auto query = 0; query < queries.size(); query += 1) {
        inspect(queries[query]).write(std::cout);
        std::cout << '\n';
    }

    std::cerr << "========== Avg ==========\n";
    inspect.header(std::cerr);
    std::cerr << '\n';
    inspect.mean().write(std::cerr);
    std::cerr << '\n';
    std::cerr << "=========================\n";
}

int main(int argc, char** argv)
{
    spdlog::drop("");
    spdlog::set_default_logger(spdlog::stderr_color_mt(""));

    std::string algorithm = "daat_or";
    bool inspect = false;
    bool safe = false;

    pisa::QueryApp app("Queries a v1 index.");
    app.add_option("--algorithm", algorithm, "Query retrieval algorithm", true);
    app.add_flag("--inspect", inspect, "Analyze query execution and stats");
    app.add_flag("--safe", safe, "Repeats without threshold if it was overestimated");
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
            auto run =
                scored_index_runner(meta,
                                    std::make_tuple(RawReader<std::uint32_t>{},
                                                    DocumentBlockedReader<::pisa::simdbp_block>{}),
                                    std::make_tuple(RawReader<std::uint8_t>{},
                                                    PayloadBlockedReader<::pisa::simdbp_block>{}));
            run([&](auto&& index) {
                if (app.is_benchmark()) {
                    benchmark(queries, resolve_algorithm(algorithm, index, VoidScorer{}, safe));
                } else if (inspect) {
                    inspect_queries(queries, resolve_inspect(algorithm, index, VoidScorer{}));
                } else {
                    evaluate(
                        queries, docmap, resolve_algorithm(algorithm, index, VoidScorer{}, safe));
                }
            });
        } else {
            auto run = index_runner(meta);
            run([&](auto&& index) {
                auto with_scorer = scorer_runner(index, make_bm25(index));
                with_scorer("bm25", [&](auto scorer) {
                    if (app.is_benchmark()) {
                        benchmark(queries, resolve_algorithm(algorithm, index, scorer, safe));
                    } else if (inspect) {
                        inspect_queries(queries, resolve_inspect(algorithm, index, scorer));
                    } else {
                        evaluate(
                            queries, docmap, resolve_algorithm(algorithm, index, scorer, safe));
                    }
                });
            });
        }
    } catch (std::exception const& error) {
        spdlog::error("{}", error.what());
    }
    return 0;
}
