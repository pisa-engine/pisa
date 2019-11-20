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
#include "v1/index_metadata.hpp"
#include "v1/maxscore.hpp"
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
using pisa::v1::resolve_yml;
using pisa::v1::VoidScorer;

using RetrievalAlgorithm =
    std::function<::pisa::topk_queue(pisa::v1::Query, ::pisa::topk_queue, tl::optional<float>)>;

template <typename Index, typename Scorer>
auto resolve_algorithm(std::string const& name, Index const& index, Scorer&& scorer)
    -> RetrievalAlgorithm
{
    if (name == "daat_or") {
        return RetrievalAlgorithm([&](pisa::v1::Query const& query,
                                      ::pisa::topk_queue topk,
                                      [[maybe_unused]] tl::optional<float> threshold) {
            return pisa::v1::daat_or(query, index, std::move(topk), std::forward<Scorer>(scorer));
        });
    }
    if (name == "maxscore") {
        return RetrievalAlgorithm([&](pisa::v1::Query const& query,
                                      ::pisa::topk_queue topk,
                                      tl::optional<float> threshold) {
            return pisa::v1::maxscore(query, index, std::move(topk), std::forward<Scorer>(scorer));
        });
    }
    spdlog::error("Unknown algorithm: {}", name);
    std::exit(1);
}

template <typename Index, typename Scorer, typename Algorithm>
void evaluate(std::vector<pisa::Query> const& queries,
              Index&& index,
              Scorer&& scorer,
              int k,
              pisa::Payload_Vector<> const& docmap,
              Algorithm&& retrieve,
              tl::optional<std::vector<float>> thresholds)
{
    auto query_idx = 0;
    for (auto const& query : queries) {
        auto threshold = thresholds.map([query_idx](auto&& vec) { return vec[query_idx]; });
        auto que = retrieve(pisa::v1::Query{query.terms}, pisa::topk_queue(k), threshold);
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

template <typename Index, typename Scorer, typename Algorithm>
void benchmark(std::vector<pisa::Query> const& queries,
               Index&& index,
               Scorer&& scorer,
               int k,
               Algorithm&& retrieve,
               tl::optional<std::vector<float>> thresholds)

{
    std::vector<double> times(queries.size(), std::numeric_limits<double>::max());
    for (auto run = 0; run < 5; run += 1) {
        for (auto query = 0; query < queries.size(); query += 1) {
            float threshold =
                thresholds.map([query](auto&& vec) { return vec[query]; }).value_or(0.0F);
            auto usecs = ::pisa::run_with_timer<std::chrono::microseconds>([&]() {
                auto que =
                    retrieve(pisa::v1::Query{queries[query].terms}, pisa::topk_queue(k), threshold);
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

int main(int argc, char** argv)
{
    std::string algorithm = "daat_or";
    tl::optional<std::string> threshold_file;
    pisa::QueryApp app("Queries a v1 index.");
    app.add_option("--algorithm", algorithm, "Query retrieval algorithm.", true);
    app.add_option("--thredsholds", algorithm, "File with (estimated) thresholds.", false);
    CLI11_PARSE(app, argc, argv);

    auto meta = IndexMetadata::from_file(resolve_yml(app.yml));
    auto stemmer = meta.stemmer ? std::make_optional(*meta.stemmer) : std::optional<std::string>{};
    if (meta.term_lexicon) {
        app.terms_file = meta.term_lexicon.value();
    }
    if (meta.document_lexicon) {
        app.documents_file = meta.document_lexicon.value();
    }

    std::vector<pisa::Query> queries;
    auto parse_query = resolve_query_parser(queries, app.terms_file, std::nullopt, stemmer);
    if (app.query_file) {
        std::ifstream is(*app.query_file);
        pisa::io::for_each_line(is, parse_query);
    } else {
        pisa::io::for_each_line(std::cin, parse_query);
    }

    if (not app.documents_file) {
        spdlog::error("Document lexicon not defined");
        std::exit(1);
    }
    auto source = std::make_shared<mio::mmap_source>(app.documents_file.value().c_str());
    auto docmap = pisa::Payload_Vector<>::from(*source);

    auto thresholds = [&threshold_file, &queries]() {
        if (threshold_file) {
            std::vector<float> thresholds;
            std::ifstream is(*threshold_file);
            pisa::io::for_each_line(
                is, [&thresholds](auto&& line) { thresholds.push_back(std::stof(line)); });
            if (thresholds.size() != queries.size()) {
                spdlog::error("Number of thresholds not equal to number of queries");
                std::exit(1);
            }
            return tl::make_optional(thresholds);
        }
        return tl::optional<std::vector<float>>{};
    }();

    if (app.precomputed) {
        auto run = scored_index_runner(meta,
                                       RawReader<std::uint32_t>{},
                                       RawReader<std::uint8_t>{},
                                       BlockedReader<::pisa::simdbp_block, true>{},
                                       BlockedReader<::pisa::simdbp_block, false>{});
        run([&](auto&& index) {
            auto retrieve = resolve_algorithm(algorithm, index, VoidScorer{});
            if (app.is_benchmark) {
                benchmark(queries, index, VoidScorer{}, app.k, retrieve, thresholds);
            } else {
                evaluate(queries, index, VoidScorer{}, app.k, docmap, retrieve, thresholds);
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
                auto retrieve = resolve_algorithm(algorithm, index, scorer);
                if (app.is_benchmark) {
                    benchmark(queries, index, scorer, app.k, retrieve, thresholds);
                } else {
                    evaluate(queries, index, scorer, app.k, docmap, retrieve, thresholds);
                }
            });
        });
    }
    return 0;
}
