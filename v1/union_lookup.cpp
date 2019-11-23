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
#include "v1/index_metadata.hpp"
#include "v1/query.hpp"
#include "v1/raw_cursor.hpp"
#include "v1/scorer/bm25.hpp"
#include "v1/scorer/runner.hpp"
#include "v1/types.hpp"
#include "v1/union_lookup.hpp"

using pisa::resolve_query_parser;
using pisa::v1::BlockedReader;
using pisa::v1::index_runner;
using pisa::v1::IndexMetadata;
using pisa::v1::Query;
using pisa::v1::RawReader;
using pisa::v1::resolve_yml;
using pisa::v1::union_lookup;
using pisa::v1::VoidScorer;

template <typename Index, typename Scorer>
void evaluate(std::vector<Query> const& queries,
              Index&& index,
              Scorer&& scorer,
              int k,
              pisa::Payload_Vector<> const& docmap,
              std::vector<std::vector<std::size_t>> essential_unigrams,
              std::vector<std::vector<std::pair<std::size_t, std::size_t>>> essential_bigrams)
{
    auto query_idx = 0;
    for (auto const& query : queries) {
        std::vector<std::size_t> uni(query.terms.size());
        std::iota(uni.begin(), uni.end(), 0);
        auto que = union_lookup(query,
                                index,
                                pisa::topk_queue(k),
                                scorer,
                                // uni, {});
                                essential_unigrams[query_idx],
                                essential_bigrams[query_idx]);
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
void benchmark(std::vector<Query> const& queries,
               Index&& index,
               Scorer&& scorer,
               int k,
               std::vector<std::vector<std::size_t>> essential_unigrams,
               std::vector<std::vector<std::pair<std::size_t, std::size_t>>> essential_bigrams)

{
    std::vector<double> times(queries.size(), std::numeric_limits<double>::max());
    for (auto run = 0; run < 5; run += 1) {
        for (auto query = 0; query < queries.size(); query += 1) {
            std::vector<std::size_t> uni(queries[query].terms.size());
            std::iota(uni.begin(), uni.end(), 0);
            auto usecs = ::pisa::run_with_timer<std::chrono::microseconds>([&]() {
                auto que = union_lookup(queries[query],
                                        index,
                                        pisa::topk_queue(k),
                                        scorer,
                                        // uni,
                                        //{});
                                        essential_unigrams[query],
                                        essential_bigrams[query]);
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
    std::string inter_filename;
    std::string threshold_file;

    pisa::QueryApp app("Queries a v1 index.");
    app.add_option("--intersections", inter_filename, "Intersections filename")->required();
    app.add_option("--thresholds", threshold_file, "File with (estimated) thresholds.")->required();
    CLI11_PARSE(app, argc, argv);

    auto meta = IndexMetadata::from_file(resolve_yml(app.yml));
    auto stemmer = meta.stemmer ? std::make_optional(*meta.stemmer) : std::optional<std::string>{};
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
        std::transform(queries.begin(), queries.end(), v1_queries.begin(), [&](auto&& query) {
            return Query{.terms = query.terms,
                         .list_selection = {},
                         .threshold = {},
                         .id =
                             [&]() {
                                 if (query.id) {
                                     return tl::make_optional(*query.id);
                                 }
                                 return tl::optional<std::string>{};
                             }(),
                         .k = app.k};
        });
        return v1_queries;
    }();

    std::ifstream is(threshold_file);
    auto queries_iter = queries.begin();
    pisa::io::for_each_line(is, [&](auto&& line) {
        if (queries_iter == queries.end()) {
            spdlog::error("Number of thresholds not equal to number of queries");
            std::exit(1);
        }
        queries_iter->threshold = tl::make_optional(std::stof(line));
        ++queries_iter;
    });
    if (queries_iter != queries.end()) {
        spdlog::error("Number of thresholds not equal to number of queries");
        std::exit(1);
    }

    auto intersections = [&]() {
        std::vector<std::vector<std::bitset<64>>> intersections;
        std::ifstream is(inter_filename);
        pisa::io::for_each_line(is, [&](auto const& query_line) {
            intersections.emplace_back();
            std::istringstream iss(query_line);
            std::transform(
                std::istream_iterator<std::string>(iss),
                std::istream_iterator<std::string>(),
                std::back_inserter(intersections.back()),
                [&](auto const& n) {
                    auto bits = std::bitset<64>(std::stoul(n));
                    if (bits.count() > 2) {
                        spdlog::error("Intersections of more than 2 terms not supported yet!");
                        std::exit(1);
                    }
                    return bits;
                });
        });
        return intersections;
    }();
    auto bitset_to_vec = [](auto bits) {
        std::vector<std::size_t> vec;
        for (auto idx = 0; idx < bits.size(); idx += 1) {
            if (bits.test(idx)) {
                vec.push_back(idx);
            }
        }
        return vec;
    };
    auto is_n_gram = [](auto n) { return [n](auto bits) { return bits.count() == n; }; };
    std::vector<std::vector<std::size_t>> unigrams =
        intersections | ranges::views::transform([&](auto&& query_intersections) {
            return query_intersections | ranges::views::filter(is_n_gram(1))
                   | ranges::views::transform([&](auto bits) { return bitset_to_vec(bits)[0]; })
                   | ranges::to_vector;
        })
        | ranges::to_vector;
    std::vector<std::vector<std::pair<std::size_t, std::size_t>>> bigrams =
        intersections | ranges::views::transform([&](auto&& query_intersections) {
            return query_intersections | ranges::views::filter(is_n_gram(2))
                   | ranges::views::transform([&](auto bits) {
                         auto vec = bitset_to_vec(bits);
                         return std::make_pair(vec[0], vec[0]);
                     })
                   | ranges::to_vector;
        })
        | ranges::to_vector;

    if (intersections.size() != queries.size()) {
        spdlog::error("Number of intersections is not equal to number of queries");
        std::exit(1);
    }

    if (not app.documents_file) {
        spdlog::error("Document lexicon not defined");
        std::exit(1);
    }
    auto source = std::make_shared<mio::mmap_source>(app.documents_file.value().c_str());
    auto docmap = pisa::Payload_Vector<>::from(*source);

    if (app.precomputed) {
        std::abort();
        // auto run = scored_index_runner(meta,
        //                               RawReader<std::uint32_t>{},
        //                               RawReader<std::uint8_t>{},
        //                               BlockedReader<::pisa::simdbp_block, true>{},
        //                               BlockedReader<::pisa::simdbp_block, false>{});
        // run([&](auto&& index) {
        //    if (app.is_benchmark) {
        //        benchmark(queries, index, VoidScorer{}, app.k, unigrams, bigrams);
        //    } else {
        //        evaluate(queries, index, VoidScorer{}, app.k, docmap, unigrams, bigrams);
        //    }
        //});
    } else {
        auto run = index_runner(meta,
                                RawReader<std::uint32_t>{},
                                BlockedReader<::pisa::simdbp_block, true>{},
                                BlockedReader<::pisa::simdbp_block, false>{});
        run([&](auto&& index) {
            auto with_scorer = scorer_runner(index, make_bm25(index));
            with_scorer("bm25", [&](auto scorer) {
                if (app.is_benchmark) {
                    benchmark(queries, index, scorer, app.k, unigrams, bigrams);
                } else {
                    evaluate(queries, index, scorer, app.k, docmap, unigrams, bigrams);
                }
            });
        });
    }
    return 0;
}
