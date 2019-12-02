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
#include "v1/analyze_query.hpp"
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
using pisa::v1::BlockedReader;
using pisa::v1::daat_or;
using pisa::v1::DaatOrAnalyzer;
using pisa::v1::index_runner;
using pisa::v1::IndexMetadata;
using pisa::v1::ListSelection;
using pisa::v1::maxscore_union_lookup;
using pisa::v1::MaxscoreAnalyzer;
using pisa::v1::MaxscoreUnionLookupAnalyzer;
using pisa::v1::Query;
using pisa::v1::QueryAnalyzer;
using pisa::v1::RawReader;
using pisa::v1::resolve_yml;
using pisa::v1::TwoPhaseUnionLookupAnalyzer;
using pisa::v1::unigram_union_lookup;
using pisa::v1::UnigramUnionLookupAnalyzer;
using pisa::v1::union_lookup;
using pisa::v1::UnionLookupAnalyzer;
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
    if (name == "two-phase-union-lookup") {
        return RetrievalAlgorithm([&](pisa::v1::Query const& query, ::pisa::topk_queue topk) {
            if (query.get_term_ids().size() > 8) {
                return pisa::v1::maxscore_union_lookup(
                    query, index, std::move(topk), std::forward<Scorer>(scorer));
            }
            return pisa::v1::two_phase_union_lookup(
                query, index, std::move(topk), std::forward<Scorer>(scorer));
        });
    }
    spdlog::error("Unknown algorithm: {}", name);
    std::exit(1);
}

template <typename Index, typename Scorer>
auto resolve_analyze(std::string const& name, Index const& index, Scorer&& scorer) -> QueryAnalyzer
{
    if (name == "daat_or") {
        return QueryAnalyzer(DaatOrAnalyzer(index, std::forward<Scorer>(scorer)));
    }
    if (name == "maxscore") {
        return QueryAnalyzer(MaxscoreAnalyzer(index, std::forward<Scorer>(scorer)));
    }
    if (name == "maxscore-union-lookup") {
        return QueryAnalyzer(
            MaxscoreUnionLookupAnalyzer<Index, std::decay_t<Scorer>>(index, scorer));
    }
    if (name == "unigram-union-lookup") {
        return QueryAnalyzer(
            UnigramUnionLookupAnalyzer<Index, std::decay_t<Scorer>>(index, scorer));
    }
    if (name == "union-lookup") {
        return QueryAnalyzer(UnionLookupAnalyzer<Index, std::decay_t<Scorer>>(index, scorer));
    }
    if (name == "two-phase-union-lookup") {
        return QueryAnalyzer(
            TwoPhaseUnionLookupAnalyzer<Index, std::decay_t<Scorer>>(index, scorer));
    }
    spdlog::error("Unknown algorithm: {}", name);
    std::exit(1);
}

void evaluate(std::vector<Query> const& queries,
              int k,
              pisa::Payload_Vector<> const& docmap,
              RetrievalAlgorithm const& retrieve)
{
    auto query_idx = 0;
    for (auto const& query : queries) {
        auto que = retrieve(query, pisa::topk_queue(k));
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

void benchmark(std::vector<Query> const& queries, int k, RetrievalAlgorithm retrieve)

{
    std::vector<double> times(queries.size(), std::numeric_limits<double>::max());
    for (auto run = 0; run < 5; run += 1) {
        for (auto query = 0; query < queries.size(); query += 1) {
            auto usecs = ::pisa::run_with_timer<std::chrono::microseconds>([&]() {
                auto que = retrieve(queries[query], pisa::topk_queue(k));
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

void analyze_queries(std::vector<Query> const& queries, QueryAnalyzer analyzer)
{
    std::vector<double> times(queries.size(), std::numeric_limits<double>::max());
    for (auto run = 0; run < 5; run += 1) {
        for (auto query = 0; query < queries.size(); query += 1) {
            analyzer(queries[query]);
        }
    }
    std::move(analyzer).summarize();
}

int main(int argc, char** argv)
{
    spdlog::drop("");
    spdlog::set_default_logger(spdlog::stderr_color_mt(""));

    std::string algorithm = "daat_or";
    tl::optional<std::string> threshold_file;
    tl::optional<std::string> inter_filename;
    bool analyze = false;

    pisa::QueryApp app("Queries a v1 index.");
    app.add_option("--algorithm", algorithm, "Query retrieval algorithm.", true);
    app.add_option("--thresholds", threshold_file, "File with (estimated) thresholds.", false);
    app.add_option("--intersections", inter_filename, "Intersections filename");
    app.add_flag("--analyze", analyze, "Analyze query execution and stats");
    CLI11_PARSE(app, argc, argv);

    try {
        auto meta = IndexMetadata::from_file(resolve_yml(app.yml));
        auto stemmer =
            meta.stemmer ? std::make_optional(*meta.stemmer) : std::optional<std::string>{};
        if (meta.term_lexicon) {
            app.terms_file = meta.term_lexicon.value();
        }
        if (meta.document_lexicon) {
            app.documents_file = meta.document_lexicon.value();
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

        if (not app.documents_file) {
            spdlog::error("Document lexicon not defined");
            std::exit(1);
        }
        auto source = std::make_shared<mio::mmap_source>(app.documents_file.value().c_str());
        auto docmap = pisa::Payload_Vector<>::from(*source);

        if (threshold_file) {
            std::ifstream is(*threshold_file);
            auto queries_iter = queries.begin();
            pisa::io::for_each_line(is, [&](auto&& line) {
                if (queries_iter == queries.end()) {
                    spdlog::error("Number of thresholds not equal to number of queries");
                    std::exit(1);
                }
                queries_iter->threshold(std::stof(line));
                ++queries_iter;
            });
            if (queries_iter != queries.end()) {
                spdlog::error("Number of thresholds not equal to number of queries");
                std::exit(1);
            }
        }

        if (inter_filename) {
            auto const intersections = pisa::v1::read_intersections(*inter_filename);
            if (intersections.size() != queries.size()) {
                spdlog::error("Number of intersections is not equal to number of queries");
                std::exit(1);
            }
            for (auto query_idx = 0; query_idx < queries.size(); query_idx += 1) {
                queries[query_idx].add_selections(gsl::make_span(intersections[query_idx]));
            }
        }

        if (app.precomputed) {
            auto run = scored_index_runner(meta,
                                           RawReader<std::uint32_t>{},
                                           RawReader<std::uint8_t>{},
                                           BlockedReader<::pisa::simdbp_block, true>{},
                                           BlockedReader<::pisa::simdbp_block, false>{});
            run([&](auto&& index) {
                if (app.is_benchmark) {
                    benchmark(queries, app.k, resolve_algorithm(algorithm, index, VoidScorer{}));
                } else if (analyze) {
                    analyze_queries(queries, resolve_analyze(algorithm, index, VoidScorer{}));
                } else {
                    evaluate(
                        queries, app.k, docmap, resolve_algorithm(algorithm, index, VoidScorer{}));
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
                    if (app.is_benchmark) {
                        benchmark(queries, app.k, resolve_algorithm(algorithm, index, scorer));
                    } else if (analyze) {
                        analyze_queries(queries, resolve_analyze(algorithm, index, scorer));
                    } else {
                        evaluate(
                            queries, app.k, docmap, resolve_algorithm(algorithm, index, scorer));
                    }
                });
            });
        }
    } catch (std::exception const& error) {
        spdlog::error("{}", error.what());
    }
    return 0;
}
