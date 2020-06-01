#include <algorithm>
#include <iostream>
#include <numeric>
#include <optional>
#include <string>

#include <CLI/CLI.hpp>
#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>
#include <mio/mmap.hpp>
#include <nlohmann/json.hpp>
#include <range/v3/view/enumerate.hpp>
#include <spdlog/sinks/null_sink.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/spdlog.h>

#include "accumulator/lazy_accumulator.hpp"
#include "app.hpp"
#include "binary_index.hpp"
#include "cursor/block_max_scored_cursor.hpp"
#include "cursor/cursor.hpp"
#include "cursor/max_scored_cursor.hpp"
#include "cursor/scored_cursor.hpp"
#include "index_types.hpp"
#include "mappable/mapper.hpp"
#include "query/algorithm.hpp"
#include "scorer/scorer.hpp"
#include "timer.hpp"
#include "topk_queue.hpp"
#include "util/util.hpp"
#include "wand_data_compressed.hpp"
#include "wand_data_raw.hpp"

using namespace pisa;
using ranges::views::enumerate;

template <typename Functor>
void op_perftest(
    Functor query_func,
    std::vector<QueryContainer> const& queries,
    std::string const& index_type,
    std::string const& query_type,
    size_t runs,
    std::uint64_t k,
    bool use_thresholds,
    bool safe)
{
    auto request_flags = RequestFlagSet::all();
    if (not use_thresholds) {
        request_flags.remove(RequestFlag::Threshold);
    }

    std::vector<std::uint64_t> query_times(queries.size(), std::numeric_limits<std::uint64_t>::max());
    std::vector<std::uint64_t> prelude_times(queries.size());
    std::vector<std::uint64_t> lookup_times(queries.size());
    std::vector<std::uint64_t> posting_times(queries.size());
    std::size_t num_reruns = 0;
    spdlog::info("Safe: {}", safe);

    for (size_t run = 0; run < runs; ++run) {
        size_t idx = 0;
        for (auto const& query: queries) {
            StaticTimer::get("prelude")->reset();
            StaticTimer::get("lookups")->reset();
            StaticTimer::get("postings")->reset();
            auto usecs = run_with_timer<std::chrono::microseconds>([&]() {
                uint64_t result = query_func(query.query(k, request_flags));
                if (safe && result < k) {
                    num_reruns += 1;
                    result =
                        query_func(query.query(k, RequestFlagSet::all() ^ RequestFlag::Threshold));
                }
                do_not_optimize_away(result);
            });
            if (usecs.count() < query_times[idx]) {
                query_times[idx] = usecs.count();
                prelude_times[idx] = StaticTimer::get("prelude")->micros();
                lookup_times[idx] = StaticTimer::get("lookups")->micros();
                posting_times[idx] = StaticTimer::get("postings")->micros();
            }
            idx += 1;
        }
    }

    auto times = nlohmann::json::array();
    std::transform(
        queries.begin(),
        queries.end(),
        query_times.begin(),
        std::back_inserter(times),
        [](auto const& query, auto time) {
            return nlohmann::json{{"query", query.to_json()}, {"time", time}};
        });

    std::sort(query_times.begin(), query_times.end());
    double avg = std::accumulate(query_times.begin(), query_times.end(), double())
        / static_cast<double>(query_times.size());
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
    spdlog::info(
        "Num. reruns: {} out of {} total runs (including warmup)", num_reruns, queries.size() * runs);

    spdlog::info(
        "Avg prelude: {}",
        std::accumulate(prelude_times.begin(), prelude_times.end(), double(0.0))
            / static_cast<double>(query_times.size()));
    spdlog::info(
        "Avg lookups: {}",
        std::accumulate(lookup_times.begin(), lookup_times.end(), double(0.0))
            / static_cast<double>(query_times.size()));
    spdlog::info(
        "Avg postings: {}",
        std::accumulate(posting_times.begin(), posting_times.end(), double(0.0))
            / static_cast<double>(query_times.size()));

    auto status = nlohmann::json{
        {"encoding", index_type},
        {"algorithm", query_type},
        {"avg", avg},
        {"q50", q50},
        {"q90", q90},
        {"q95", q95},
        {"q99", q99},
        {"runs", runs},
        {"k", k},
        {"use_thresholds", use_thresholds},
        {"safe", safe},
        {"reruns", num_reruns / runs},
        {"queries", times}};
    std::cout << status.dump() << '\n';
}

template <typename IndexType, typename WandType>
void perftest(
    const std::string& index_filename,
    const std::optional<std::string>& wand_data_filename,
    const std::vector<QueryContainer>& queries,
    std::string const& type,
    std::string const& query_type,
    uint64_t k,
    const ScorerParams& scorer_params,
    bool use_thresholds,
    bool safe,
    std::optional<std::string>& pair_index_path)
{
    IndexType index;
    spdlog::info("Loading index from {}", index_filename);
    mio::mmap_source m(index_filename.c_str());
    mapper::map(index, m);

    spdlog::info("Warming up posting lists");
    std::unordered_set<term_id_type> warmed_up;
    for (auto&& query: queries) {
        for (auto t: *query.term_ids()) {
            if (!warmed_up.count(t)) {
                index.warmup(t);
                warmed_up.insert(t);
            }
        }
    }

    WandType wdata;

    std::vector<std::string> query_types;
    boost::algorithm::split(query_types, query_type, boost::is_any_of(":"));
    mio::mmap_source md;
    if (wand_data_filename) {
        std::error_code error;
        md.map(*wand_data_filename, error);
        if (error) {
            std::cerr << "error mapping file: " << error.message() << ", exiting..." << std::endl;
            throw std::runtime_error("Error opening file");
        }
        mapper::map(wdata, md, mapper::map_flags::warmup);
    }

    using pair_index_type = PairIndex<block_freq_index<simdbp_block, false, IndexArity::Binary>>;
    auto pair_index = [&]() -> std::optional<pair_index_type> {
        if (pair_index_path) {
            return pair_index_type::load(*pair_index_path);
        }
        return std::nullopt;
    }();

    auto scorer = scorer::from_params(scorer_params, wdata);

    spdlog::info("Performing {} queries", type);
    spdlog::info("K: {}", k);

    for (auto&& t: query_types) {
        spdlog::info("Query type: {}", t);
        std::function<uint64_t(QueryRequest const&)> query_fun;
        if (t == "and") {
            query_fun = [&](QueryRequest const& query) {
                and_query and_q;
                return and_q(make_cursors(index, query), index.num_docs()).size();
            };
        } else if (t == "or") {
            query_fun = [&](QueryRequest const& query) {
                or_query<false> or_q;
                return or_q(make_cursors(index, query), index.num_docs());
            };
        } else if (t == "or_freq") {
            query_fun = [&](QueryRequest const& query) {
                or_query<true> or_q;
                return or_q(make_cursors(index, query), index.num_docs());
            };
        } else if (t == "wand" && wand_data_filename) {
            query_fun = [&](QueryRequest const& query) {
                topk_queue topk(k);
                topk.set_threshold(query.threshold().value_or(0));
                wand_query wand_q(topk);
                wand_q(make_max_scored_cursors(index, wdata, *scorer, query), index.num_docs());
                topk.finalize();
                return topk.topk().size();
            };
        } else if (t == "block_max_wand" && wand_data_filename) {
            query_fun = [&](QueryRequest const& query) {
                topk_queue topk(k);
                topk.set_threshold(query.threshold().value_or(0));
                block_max_wand_query block_max_wand_q(topk);
                block_max_wand_q(
                    make_block_max_scored_cursors(index, wdata, *scorer, query), index.num_docs());
                topk.finalize();
                return topk.topk().size();
            };
        } else if (t == "block_max_maxscore" && wand_data_filename) {
            query_fun = [&](QueryRequest const& query) {
                topk_queue topk(k);
                topk.set_threshold(query.threshold().value_or(0));
                block_max_maxscore_query block_max_maxscore_q(topk);
                block_max_maxscore_q(
                    make_block_max_scored_cursors(index, wdata, *scorer, query), index.num_docs());
                topk.finalize();
                return topk.topk().size();
            };
        } else if (t == "ranked_and" && wand_data_filename) {
            query_fun = [&](QueryRequest const& query) {
                topk_queue topk(k);
                topk.set_threshold(query.threshold().value_or(0));
                ranked_and_query ranked_and_q(topk);
                ranked_and_q(make_scored_cursors(index, *scorer, query), index.num_docs());
                topk.finalize();
                return topk.topk().size();
            };
        } else if (t == "block_max_ranked_and" && wand_data_filename) {
            query_fun = [&](QueryRequest const& query) {
                topk_queue topk(k);
                topk.set_threshold(query.threshold().value_or(0));
                block_max_ranked_and_query block_max_ranked_and_q(topk);
                block_max_ranked_and_q(
                    make_block_max_scored_cursors(index, wdata, *scorer, query), index.num_docs());
                topk.finalize();
                return topk.topk().size();
            };
        } else if (t == "ranked_or" && wand_data_filename) {
            query_fun = [&](QueryRequest const& query) {
                topk_queue topk(k);
                topk.set_threshold(query.threshold().value_or(0));
                ranked_or_query ranked_or_q(topk);
                ranked_or_q(make_scored_cursors(index, *scorer, query), index.num_docs());
                topk.finalize();
                return topk.topk().size();
            };
        } else if (t == "maxscore" && wand_data_filename) {
            query_fun = [&](QueryRequest const& query) {
                topk_queue topk(k);
                topk.set_threshold(query.threshold().value_or(0));
                maxscore_query maxscore_q(topk);
                maxscore_q(make_max_scored_cursors(index, wdata, *scorer, query), index.num_docs());
                topk.finalize();
                return topk.topk().size();
            };
        } else if (t == "ranked_or_taat" && wand_data_filename) {
            Simple_Accumulator accumulator(index.num_docs());
            topk_queue topk(k);
            ranked_or_taat_query ranked_or_taat_q(topk);
            query_fun = [&, ranked_or_taat_q, accumulator](QueryRequest const& query) mutable {
                topk.set_threshold(query.threshold().value_or(0));
                ranked_or_taat_q(
                    make_scored_cursors(index, *scorer, query), index.num_docs(), accumulator);
                topk.finalize();
                return topk.topk().size();
            };
        } else if (t == "ranked_or_taat_lazy" && wand_data_filename) {
            Lazy_Accumulator<4> accumulator(index.num_docs());
            topk_queue topk(k);
            ranked_or_taat_query ranked_or_taat_q(topk);
            query_fun = [&, ranked_or_taat_q, accumulator](QueryRequest const& query) mutable {
                topk.set_threshold(query.threshold().value_or(0));
                ranked_or_taat_q(
                    make_scored_cursors(index, *scorer, query), index.num_docs(), accumulator);
                topk.finalize();
                return topk.topk().size();
            };
        } else if (t == "maxscore-uni" && wand_data_filename) {
            query_fun = [&](QueryRequest const& query) {
                topk_queue topk(k);
                topk.set_threshold(query.threshold().value_or(0));
                maxscore_uni_query q(topk);
                q(make_block_max_scored_cursors(index, wdata, *scorer, query), index.num_docs());
                topk.finalize();
                return topk.topk().size();
            };
        } else if (t == "maxscore-inter" && wand_data_filename) {
            query_fun = [&](QueryRequest const& query) {
                topk_queue topk(k);
                topk.set_threshold(query.threshold().value_or(0));
                if (not query.selection()) {
                    spdlog::error("maxscore_inter_query requires posting list selections");
                    std::exit(1);
                }
                auto selection = *query.selection();
                if (selection.selected_pairs.empty()) {
                    maxscore_query maxscore_q(topk);
                    maxscore_q(
                        make_max_scored_cursors(index, wdata, *scorer, query), index.num_docs());
                    topk.finalize();
                    return topk.topk().size();
                }
                maxscore_inter_query q(topk);
                if (not pair_index) {
                    spdlog::error("Must provide pair index for maxscore-inter");
                    std::exit(1);
                }
                q(query, index, wdata, *pair_index, *scorer, index.num_docs());
                topk.finalize();
                return topk.topk().size();
            };
        } else if (t == "maxscore-inter-eager" && wand_data_filename) {
            query_fun = [&](QueryRequest const& query) {
                topk_queue topk(k);
                topk.set_threshold(query.threshold().value_or(0));
                if (not query.selection()) {
                    spdlog::error("maxscore_inter_query requires posting list selections");
                    std::exit(1);
                }
                auto selection = *query.selection();
                if (selection.selected_pairs.empty()) {
                    maxscore_query maxscore_q(topk);
                    maxscore_q(
                        make_max_scored_cursors(index, wdata, *scorer, query), index.num_docs());
                    topk.finalize();
                    return topk.topk().size();
                }
                maxscore_inter_eager_query q(topk);
                if (not pair_index) {
                    spdlog::error("Must provide pair index for maxscore-inter");
                    std::exit(1);
                }
                q(query, index, wdata, *pair_index, *scorer, index.num_docs());
                topk.finalize();
                return topk.topk().size();
            };
        } else if (t == "maxscore-inter-opt" && wand_data_filename) {
            query_fun = [&](QueryRequest const& query) {
                topk_queue topk(k);
                topk.set_threshold(query.threshold().value_or(0));
                if (not query.selection()) {
                    spdlog::error("maxscore-inter-opt requires posting list selections");
                    std::exit(1);
                }
                auto selection = *query.selection();
                if (selection.selected_pairs.empty()) {
                    maxscore_query maxscore_q(topk);
                    maxscore_q(
                        make_max_scored_cursors(index, wdata, *scorer, query), index.num_docs());
                    topk.finalize();
                    return topk.topk().size();
                }
                if (not pair_index) {
                    spdlog::error("Must provide pair index for maxscore-inter");
                    std::exit(1);
                }
                maxscore_inter_opt_query q(topk);
                q(query, index, wdata, *pair_index, *scorer, index.num_docs());
                topk.finalize();
                return topk.topk().size();
            };
        } else {
            spdlog::error("Unsupported query type: {}", t);
            break;
        }
        op_perftest(query_fun, queries, type, t, 3, k, use_thresholds, safe);
    }
}

using wand_raw_index = wand_data<wand_data_raw>;
using wand_uniform_index = wand_data<wand_data_compressed<>>;
using wand_uniform_index_quantized = wand_data<wand_data_compressed<PayloadType::Quantized>>;

int main(int argc, const char** argv)
{
    spdlog::drop("");
    spdlog::set_default_logger(spdlog::stderr_color_mt(""));

    bool silent = false;
    bool safe = false;
    bool quantized = false;
    bool use_thresholds = false;
    std::optional<std::string> pair_index_path{};

    App<arg::Index, arg::WandData<arg::WandMode::Optional>, arg::Query<arg::QueryMode::Ranked>, arg::Algorithm, arg::Scorer>
        app{"Benchmarks queries on a given index."};
    app.add_flag("--quantized", quantized, "Quantized scores");
    app.add_flag("--silent", silent, "Suppress logging");
    auto* thresholds_option = app.add_flag(
        "--use-thresholds",
        use_thresholds,
        "Initialize top-k queue with threshold passed as part of a query object");
    app.add_flag("--safe", safe, "Rerun if not enough results with pruning.")->needs(thresholds_option);
    app.add_option("--pair-index", pair_index_path, "Path to pair index.");
    CLI11_PARSE(app, argc, argv);

    if (silent) {
        spdlog::set_default_logger(spdlog::create<spdlog::sinks::null_sink_mt>("stderr"));
    } else {
        spdlog::set_default_logger(spdlog::stderr_color_mt("stderr"));
    }

    std::vector<pisa::QueryContainer> queries;
    try {
        auto reader = app.resolved_query_reader();
        reader.for_each([&](auto&& query) {
            // if (not query.selection(app.k())->selected_pairs.empty()) {
            queries.push_back(query);
            //}
        });
    } catch (pisa::MissingResolverError err) {
        spdlog::error("Unresoved queries (without IDs) require term lexicon.");
        std::exit(1);
    } catch (std::runtime_error const& err) {
        spdlog::error(err.what());
        std::exit(1);
    }

    for (auto& query: queries) {
        query.add_threshold(app.k(), *query.threshold(app.k()) - 0.001);
    }

    auto params = std::make_tuple(
        app.index_filename(),
        app.wand_data_path(),
        queries,
        app.index_encoding(),
        app.algorithm(),
        app.k(),
        app.scorer_params(),
        use_thresholds,
        safe,
        pair_index_path);
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
