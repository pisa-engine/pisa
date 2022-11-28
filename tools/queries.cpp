#include <algorithm>
#include <iostream>
#include <numeric>
#include <optional>
#include <string>

#include <CLI/CLI.hpp>
#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>
#include <range/v3/view/enumerate.hpp>
#include <spdlog/sinks/null_sink.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/spdlog.h>

#include "accumulator/lazy_accumulator.hpp"
#include "app.hpp"
#include "cursor/block_max_scored_cursor.hpp"
#include "cursor/cursor.hpp"
#include "cursor/max_scored_cursor.hpp"
#include "cursor/scored_cursor.hpp"
#include "index_types.hpp"
#include "mappable/mapper.hpp"
#include "memory_source.hpp"
#include "query/algorithm.hpp"
#include "scorer/scorer.hpp"
#include "timer.hpp"
#include "topk_queue.hpp"
#include "type_alias.hpp"
#include "util/util.hpp"
#include "wand_data_compressed.hpp"
#include "wand_data_raw.hpp"

using namespace pisa;
using ranges::views::enumerate;

template <typename Fn>
void extract_times(
    Fn fn,
    std::vector<Query> const& queries,
    std::vector<Score> const& thresholds,
    std::string const& index_type,
    std::string const& query_type,
    size_t runs,
    std::ostream& os)
{
    std::vector<std::size_t> times(runs);
    for (auto&& [qid, query]: enumerate(queries)) {
        do_not_optimize_away(fn(query, thresholds[qid]));
        std::generate(times.begin(), times.end(), [&fn, &q = query, &t = thresholds[qid]]() {
            return run_with_timer<std::chrono::microseconds>(
                       [&]() { do_not_optimize_away(fn(q, t)); })
                .count();
        });
        auto mean = std::accumulate(times.begin(), times.end(), std::size_t{0}, std::plus<>()) / runs;
        os << fmt::format("{}\t{}\n", query.id.value_or(std::to_string(qid)), mean);
    }
}

template <typename Functor>
void op_perftest(
    Functor query_func,
    std::vector<Query> const& queries,
    std::vector<Score> const& thresholds,
    std::string const& index_type,
    std::string const& query_type,
    size_t runs,
    std::uint64_t k,
    bool safe)
{
    std::vector<double> query_times;
    std::size_t num_reruns = 0;
    spdlog::info("Safe: {}", safe);

    for (size_t run = 0; run <= runs; ++run) {
        size_t idx = 0;
        for (auto const& query: queries) {
            auto usecs = run_with_timer<std::chrono::microseconds>([&]() {
                uint64_t result = query_func(query, thresholds[idx]);
                if (safe && result < k) {
                    num_reruns += 1;
                    result = query_func(query, 0);
                }
                do_not_optimize_away(result);
            });
            if (run != 0) {  // first run is not timed
                query_times.push_back(usecs.count());
            }
            idx += 1;
        }
    }

    if (false) {
        for (auto t: query_times) {
            std::cout << (t / 1000) << std::endl;
        }
    } else {
        std::sort(query_times.begin(), query_times.end());
        double avg =
            std::accumulate(query_times.begin(), query_times.end(), double()) / query_times.size();
        double q50 = query_times[query_times.size() / 2];
        double q90 = query_times[90 * query_times.size() / 100];
        double q95 = query_times[95 * query_times.size() / 100];
        double q99 = query_times[99 * query_times.size() / 100];

        spdlog::info("---- {} {}", index_type, query_type);
        spdlog::info("Mean: {}", avg);
        spdlog::info("50% quantile: {}", q50);
        spdlog::info("90% quantile: {}", q90);
        spdlog::info("95% quantile: {}", q95);
        spdlog::info("99% quantile: {}", q99);
        spdlog::info("Num. reruns: {}", num_reruns);

        stats_line()("type", index_type)("query", query_type)("avg", avg)("q50", q50)("q90", q90)(
            "q95", q95)("q99", q99);
    }
}

template <typename IndexType, typename WandType>
void perftest(
    const std::string& index_filename,
    const std::optional<std::string>& wand_data_filename,
    const std::vector<Query>& queries,
    const std::optional<std::string>& thresholds_filename,
    std::string const& type,
    std::string const& query_type,
    uint64_t k,
    const ScorerParams& scorer_params,
    const bool weighted,
    bool extract,
    bool safe)
{
    spdlog::info("Loading index from {}", index_filename);
    IndexType index(MemorySource::mapped_file(index_filename));

    spdlog::info("Warming up posting lists");
    std::unordered_set<term_id_type> warmed_up;
    for (auto const& q: queries) {
        for (auto t: q.terms) {
            if (!warmed_up.count(t)) {
                index.warmup(t);
                warmed_up.insert(t);
            }
        }
    }

    WandType const wdata = [&] {
        if (wand_data_filename) {
            return WandType(MemorySource::mapped_file(*wand_data_filename));
        }
        return WandType{};
    }();

    std::vector<Score> thresholds(queries.size(), 0.0);
    if (thresholds_filename) {
        std::string t;
        std::ifstream tin(*thresholds_filename);
        size_t idx = 0;
        while (std::getline(tin, t)) {
            thresholds[idx] = std::stof(t);
            idx += 1;
        }
        if (idx != queries.size()) {
            throw std::invalid_argument("Invalid thresholds file.");
        }
    }

    auto scorer = scorer::from_params(scorer_params, wdata);

    spdlog::info("Performing {} queries", type);
    spdlog::info("K: {}", k);

    std::vector<std::string> query_types;
    boost::algorithm::split(query_types, query_type, boost::is_any_of(":"));

    for (auto&& t: query_types) {
        spdlog::info("Query type: {}", t);
        std::function<uint64_t(Query, Score)> query_fun;
        if (t == "and") {
            query_fun = [&](Query query, Score) {
                and_query and_q;
                return and_q(make_cursors(index, query), index.num_docs()).size();
            };
        } else if (t == "or") {
            query_fun = [&](Query query, Score) {
                or_query<false> or_q;
                return or_q(make_cursors(index, query), index.num_docs());
            };
        } else if (t == "or_freq") {
            query_fun = [&](Query query, Score) {
                or_query<true> or_q;
                return or_q(make_cursors(index, query), index.num_docs());
            };
        } else if (t == "wand" && wand_data_filename) {
            query_fun = [&](Query query, Score threshold) {
                topk_queue topk(k, threshold);
                wand_query wand_q(topk);
                wand_q(
                    make_max_scored_cursors(index, wdata, *scorer, query, weighted),
                    index.num_docs());
                topk.finalize();
                return topk.topk().size();
            };
        } else if (t == "block_max_wand" && wand_data_filename) {
            query_fun = [&](Query query, Score threshold) {
                topk_queue topk(k, threshold);
                block_max_wand_query block_max_wand_q(topk);
                block_max_wand_q(
                    make_block_max_scored_cursors(index, wdata, *scorer, query, weighted),
                    index.num_docs());
                topk.finalize();
                return topk.topk().size();
            };
        } else if (t == "block_max_maxscore" && wand_data_filename) {
            query_fun = [&](Query query, Score threshold) {
                topk_queue topk(k, threshold);
                block_max_maxscore_query block_max_maxscore_q(topk);
                block_max_maxscore_q(
                    make_block_max_scored_cursors(index, wdata, *scorer, query, weighted),
                    index.num_docs());
                topk.finalize();
                return topk.topk().size();
            };
        } else if (t == "ranked_and" && wand_data_filename) {
            query_fun = [&](Query query, Score threshold) {
                topk_queue topk(k, threshold);
                ranked_and_query ranked_and_q(topk);
                ranked_and_q(make_scored_cursors(index, *scorer, query, weighted), index.num_docs());
                topk.finalize();
                return topk.topk().size();
            };
        } else if (t == "block_max_ranked_and" && wand_data_filename) {
            query_fun = [&](Query query, Score threshold) {
                topk_queue topk(k, threshold);
                block_max_ranked_and_query block_max_ranked_and_q(topk);
                block_max_ranked_and_q(
                    make_block_max_scored_cursors(index, wdata, *scorer, query, weighted),
                    index.num_docs());
                topk.finalize();
                return topk.topk().size();
            };
        } else if (t == "ranked_or" && wand_data_filename) {
            query_fun = [&](Query query, Score threshold) {
                topk_queue topk(k, threshold);
                ranked_or_query ranked_or_q(topk);
                ranked_or_q(make_scored_cursors(index, *scorer, query, weighted), index.num_docs());
                topk.finalize();
                return topk.topk().size();
            };
        } else if (t == "maxscore" && wand_data_filename) {
            query_fun = [&](Query query, Score threshold) {
                topk_queue topk(k, threshold);
                maxscore_query maxscore_q(topk);
                maxscore_q(
                    make_max_scored_cursors(index, wdata, *scorer, query, weighted),
                    index.num_docs());
                topk.finalize();
                return topk.topk().size();
            };
        } else if (t == "ranked_or_taat" && wand_data_filename) {
            Simple_Accumulator accumulator(index.num_docs());
            topk_queue topk(k);
            ranked_or_taat_query ranked_or_taat_q(topk);
            query_fun = [&, ranked_or_taat_q, accumulator](Query query, Score threshold) mutable {
                topk.clear(threshold);
                ranked_or_taat_q(
                    make_scored_cursors(index, *scorer, query, weighted),
                    index.num_docs(),
                    accumulator);
                topk.finalize();
                return topk.topk().size();
            };
        } else if (t == "ranked_or_taat_lazy" && wand_data_filename) {
            Lazy_Accumulator<4> accumulator(index.num_docs());
            topk_queue topk(k);
            ranked_or_taat_query ranked_or_taat_q(topk);
            query_fun = [&, ranked_or_taat_q, accumulator](Query query, Score threshold) mutable {
                topk.clear(threshold);
                ranked_or_taat_q(
                    make_scored_cursors(index, *scorer, query, weighted),
                    index.num_docs(),
                    accumulator);
                topk.finalize();
                return topk.topk().size();
            };
        } else {
            spdlog::error("Unsupported query type: {}", t);
            break;
        }
        if (extract) {
            extract_times(query_fun, queries, thresholds, type, t, 2, std::cout);
        } else {
            op_perftest(query_fun, queries, thresholds, type, t, 2, k, safe);
        }
    }
}

using wand_raw_index = wand_data<wand_data_raw>;
using wand_uniform_index = wand_data<wand_data_compressed<>>;
using wand_uniform_index_quantized = wand_data<wand_data_compressed<PayloadType::Quantized>>;

int main(int argc, const char** argv)
{
    bool extract = false;
    bool safe = false;
    bool quantized = false;

    App<arg::Index,
        arg::WandData<arg::WandMode::Optional>,
        arg::Query<arg::QueryMode::Ranked>,
        arg::Algorithm,
        arg::Scorer,
        arg::Thresholds,
        arg::LogLevel>
        app{"Benchmarks queries on a given index."};
    app.add_flag("--quantized", quantized, "Quantized scores");
    app.add_flag("--extract", extract, "Extract individual query times");
    app.add_flag("--safe", safe, "Rerun if not enough results with pruning.")
        ->needs(app.thresholds_option());
    CLI11_PARSE(app, argc, argv);

    spdlog::set_default_logger(spdlog::stderr_color_mt("stderr"));
    spdlog::set_level(app.log_level());
    if (extract) {
        std::cout << "qid\tusec\n";
    }

    auto params = std::make_tuple(
        app.index_filename(),
        app.wand_data_path(),
        app.queries(),
        app.thresholds_file(),
        app.index_encoding(),
        app.algorithm(),
        app.k(),
        app.scorer_params(),
        app.weighted(),
        extract,
        safe);
    /**/
    if (false) {
#define LOOP_BODY(R, DATA, T)                                                                        \
    }                                                                                                \
    else if (app.index_encoding() == BOOST_PP_STRINGIZE(T))                                          \
    {                                                                                                \
        if (app.is_wand_compressed()) {                                                              \
            if (quantized) {                                                                         \
                std::apply(perftest<BOOST_PP_CAT(T, _index), wand_uniform_index_quantized>, params); \
            } else {                                                                                 \
                std::apply(perftest<BOOST_PP_CAT(T, _index), wand_uniform_index>, params);           \
            }                                                                                        \
        } else {                                                                                     \
            std::apply(perftest<BOOST_PP_CAT(T, _index), wand_raw_index>, params);                   \
        }
        /**/
        BOOST_PP_SEQ_FOR_EACH(LOOP_BODY, _, PISA_INDEX_TYPES);
#undef LOOP_BODY

    } else {
        spdlog::error("Unknown type {}", app.index_encoding());
    }
}
