#include <algorithm>
#include <fstream>
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
#include "accumulator/simple_accumulator.hpp"
#include "app.hpp"
#include "cursor/block_max_scored_cursor.hpp"
#include "cursor/cursor.hpp"
#include "cursor/max_scored_cursor.hpp"
#include "cursor/scored_cursor.hpp"
#include "index_types.hpp"
#include "memory_source.hpp"
#include "query/algorithm/and_query.hpp"
#include "query/algorithm/block_max_maxscore_query.hpp"
#include "query/algorithm/block_max_ranked_and_query.hpp"
#include "query/algorithm/block_max_wand_query.hpp"
#include "query/algorithm/maxscore_query.hpp"
#include "query/algorithm/or_query.hpp"
#include "query/algorithm/ranked_and_query.hpp"
#include "query/algorithm/ranked_or_query.hpp"
#include "query/algorithm/ranked_or_taat_query.hpp"
#include "query/algorithm/wand_query.hpp"
#include "scorer/scorer.hpp"
#include "timer.hpp"
#include "topk_queue.hpp"
#include "type_alias.hpp"
#include "util/do_not_optimize_away.hpp"
#include "util/util.hpp"
#include "wand_data.hpp"
#include "wand_data_compressed.hpp"
#include "wand_data_raw.hpp"

using namespace pisa;
using ranges::views::enumerate;

enum class AggregationType { None = 0, Min = 1, Mean = 2, Median = 3, Max = 4 };

[[nodiscard]] auto to_string(AggregationType type) -> std::string {
    switch (type) {
    case AggregationType::None: return "none";
    case AggregationType::Min: return "min";
    case AggregationType::Mean: return "mean";
    case AggregationType::Median: return "median";
    case AggregationType::Max: return "max";
    }
    return "unknown";
}

std::vector<std::size_t> aggregate_and_sort_times_per_query(
    AggregationType aggregation_type, std::vector<std::vector<std::size_t>> const& times_per_query
) {
    std::vector<std::size_t> aggregated_query_times;
    if (aggregation_type == AggregationType::None) {
        for (auto const& query_times: times_per_query) {
            for (auto t: query_times) {
                aggregated_query_times.push_back(t);
            }
        }
    } else if (aggregation_type == AggregationType::Min) {
        for (auto const& query_times: times_per_query) {
            aggregated_query_times.push_back(*std::min_element(query_times.begin(), query_times.end()));
        }
    } else if (aggregation_type == AggregationType::Mean) {
        for (auto const& query_times: times_per_query) {
            double sum = std::accumulate(query_times.begin(), query_times.end(), double());
            double mean = sum / query_times.size();
            aggregated_query_times.push_back(mean);
        }
    } else if (aggregation_type == AggregationType::Median) {
        for (auto const& query_times: times_per_query) {
            auto sorted_query_times = query_times;
            std::sort(sorted_query_times.begin(), sorted_query_times.end());
            std::size_t sample_count = sorted_query_times.size();
            double median = 0;
            if (sample_count % 2 == 1) {
                median = sorted_query_times[sample_count / 2];
            } else {
                median =
                    (sorted_query_times[sample_count / 2] + sorted_query_times[sample_count / 2 - 1])
                    / 2;
            }
            aggregated_query_times.push_back(median);
        }
    } else if (aggregation_type == AggregationType::Max) {
        for (auto const& query_times: times_per_query) {
            aggregated_query_times.push_back(*std::max_element(query_times.begin(), query_times.end()));
        }
    }
    std::sort(aggregated_query_times.begin(), aggregated_query_times.end());
    return aggregated_query_times;
}

template <typename Fn>
void extract_times(
    Fn query_func,
    std::vector<Query> const& queries,
    std::vector<Score> const& thresholds,
    std::string const& index_type,
    std::string const& query_type,
    size_t runs,
    std::uint64_t k,
    bool safe,
    std::ostream* os = nullptr
) {
    std::vector<std::vector<std::size_t>> times_per_query(
        queries.size(), std::vector<std::size_t>(runs)
    );
    std::size_t corrective_rerun_count = 0;

    // Note: each query is measured once per run, so the set of queries is
    // measured independently in each run.
    for (size_t run = 0; run <= runs; ++run) {
        size_t query_idx = 0;
        for (auto const& query: queries) {
            auto usecs = run_with_timer<std::chrono::microseconds>([&]() {
                uint64_t result = query_func(query, thresholds[query_idx]);
                if (safe && result < k) {
                    corrective_rerun_count += 1;
                    result = query_func(query, 0);
                }
                do_not_optimize_away(result);
            });
            if (run != 0) {  // first run is not timed
                times_per_query[query_idx][run - 1] = usecs.count();
            }
            query_idx += 1;
        }
    }

    // Print JSON summary
    std::cout << "{\n"
              << "  \"encoding\": \"" << index_type << "\",\n"
              << "  \"algorithm\": \"" << query_type << "\",\n"
              << "  \"runs\": " << runs << ",\n"
              << "  \"k\": " << k << ",\n"
              << "  \"safe\": " << (safe ? "true" : "false") << ",\n"
              << "  \"corrective_reruns\": " << corrective_rerun_count << ",\n"
              << "  \"query_aggregation\": {\n";

    auto print_aggregated_query_times = [&](AggregationType agg_type, bool is_last=false) {
        auto query_times = aggregate_and_sort_times_per_query(agg_type, times_per_query);
        auto agg_name = to_string(agg_type);

        double mean =
            std::accumulate(query_times.begin(), query_times.end(), double()) / query_times.size();
        double q50 = query_times[query_times.size() / 2];
        double q90 = query_times[90 * query_times.size() / 100];
        double q95 = query_times[95 * query_times.size() / 100];
        double q99 = query_times[99 * query_times.size() / 100];

        std::cout << "    \"" << agg_name << "\": {"
                  << "\"mean\": " << mean << ", "
                  << "\"q50\": " << q50 << ", "
                  << "\"q90\": " << q90 << ", "
                  << "\"q95\": " << q95 << ", "
                  << "\"q99\": " << q99 << "}";
        if (!is_last) {
            std::cout << ",\n";
        }
    };

    print_aggregated_query_times(AggregationType::None);
    print_aggregated_query_times(AggregationType::Min);
    print_aggregated_query_times(AggregationType::Mean);
    print_aggregated_query_times(AggregationType::Median);
    print_aggregated_query_times(AggregationType::Max, true);
    std::cout << "\n  }\n}\n";

    // Save times per query (if required)
    if (os != nullptr) {
        *os << "qid";
        for (size_t i = 1; i <= runs; ++i) {
            *os << fmt::format("\tusec{}", i);
        }
        *os << "\n";
        for (auto&& [query_idx, query]: enumerate(queries)) {
            *os << fmt::format("{}", query.id().value_or(std::to_string(query_idx)));
            for (auto t: times_per_query[query_idx]) {
                *os << fmt::format("\t{}", t);
            }
            *os << "\n";
        }
    }
}

template <typename IndexType, typename WandType>
void perftest(
    IndexType const* index_ptr,
    const std::optional<std::string>& wand_data_filename,
    const std::vector<Query>& queries,
    const std::optional<std::string>& thresholds_filename,
    std::string const& type,
    std::string const& query_type,
    uint64_t k,
    const ScorerParams& scorer_params,
    const bool weighted,
    bool safe,
    std::size_t runs,
    std::ostream* output
) {
    auto const& index = *index_ptr;
    spdlog::info("Warming up posting lists...");
    std::unordered_set<TermId> warmed_up;
    for (auto const& q: queries) {
        for (auto [t, _]: q.terms()) {
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
    std::vector<std::string> query_types;
    boost::algorithm::split(query_types, query_type, boost::is_any_of(":"));

    for (auto&& t: query_types) {
        spdlog::info("Performing {} runs for '{}' queries...", runs, t);
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
                    make_max_scored_cursors(index, wdata, *scorer, query, weighted), index.num_docs()
                );
                topk.finalize();
                return topk.topk().size();
            };
        } else if (t == "block_max_wand" && wand_data_filename) {
            query_fun = [&](Query query, Score threshold) {
                topk_queue topk(k, threshold);
                block_max_wand_query block_max_wand_q(topk);
                block_max_wand_q(
                    make_block_max_scored_cursors(index, wdata, *scorer, query, weighted),
                    index.num_docs()
                );
                topk.finalize();
                return topk.topk().size();
            };
        } else if (t == "block_max_maxscore" && wand_data_filename) {
            query_fun = [&](Query query, Score threshold) {
                topk_queue topk(k, threshold);
                block_max_maxscore_query block_max_maxscore_q(topk);
                block_max_maxscore_q(
                    make_block_max_scored_cursors(index, wdata, *scorer, query, weighted),
                    index.num_docs()
                );
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
                    index.num_docs()
                );
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
                    make_max_scored_cursors(index, wdata, *scorer, query, weighted), index.num_docs()
                );
                topk.finalize();
                return topk.topk().size();
            };
        } else if (t == "ranked_or_taat" && wand_data_filename) {
            SimpleAccumulator accumulator(index.num_docs());
            topk_queue topk(k);
            query_fun = [&, topk, accumulator](Query query, Score threshold) mutable {
                ranked_or_taat_query ranked_or_taat_q(topk);
                topk.clear(threshold);
                ranked_or_taat_q(
                    make_scored_cursors(index, *scorer, query, weighted), index.num_docs(), accumulator
                );
                topk.finalize();
                return topk.topk().size();
            };
        } else if (t == "ranked_or_taat_lazy" && wand_data_filename) {
            LazyAccumulator<4> accumulator(index.num_docs());
            topk_queue topk(k);
            query_fun = [&, topk, accumulator](Query query, Score threshold) mutable {
                ranked_or_taat_query ranked_or_taat_q(topk);
                topk.clear(threshold);
                ranked_or_taat_q(
                    make_scored_cursors(index, *scorer, query, weighted), index.num_docs(), accumulator
                );
                topk.finalize();
                return topk.topk().size();
            };
        } else {
            spdlog::error("Unsupported query type: {}", t);
            break;
        }
        extract_times(
            query_fun, queries, thresholds, type, t, runs, k, safe, output
        );
    }
}

using wand_raw_index = wand_data<wand_data_raw>;
using wand_uniform_index = wand_data<wand_data_compressed<>>;
using wand_uniform_index_quantized = wand_data<wand_data_compressed<PayloadType::Quantized>>;

int main(int argc, const char** argv) {
    bool safe = false;
    bool quantized = false;
    std::size_t runs = 0;
    std::optional<std::string> output_filename;

    App<arg::Index,
        arg::WandData<arg::WandMode::Optional>,
        arg::Query<arg::QueryMode::Ranked>,
        arg::Algorithm,
        arg::Scorer,
        arg::Thresholds,
        arg::LogLevel>
        app{"Benchmarks queries on a given index."};
    app.add_flag("--quantized", quantized, "Quantized scores");
    app.add_flag("--safe", safe, "Rerun if not enough results with pruning.")
        ->needs(app.thresholds_option());
    app.add_option("--runs", runs, "Number of runs per query")->default_val(3)->check(CLI::PositiveNumber);
    app.add_option("-o,--output", output_filename, "Output file for query timing data");
    CLI11_PARSE(app, argc, argv);

    spdlog::set_default_logger(spdlog::stderr_color_mt("stderr"));
    spdlog::set_level(app.log_level());

    std::ofstream output_file;
    std::ostream* output = nullptr;
    if (output_filename) {
        output_file.open(*output_filename);
        if (!output_file) {
            spdlog::error("Failed to open data output file: {}", *output_filename);
            return 1;
        }
        output = &output_file;
    }

    run_for_index(
        app.index_encoding(), MemorySource::mapped_file(app.index_filename()), [&](auto index) {
            using Index = std::decay_t<decltype(index)>;
            auto params = std::make_tuple(
                &index,
                app.wand_data_path(),
                app.queries(),
                app.thresholds_file(),
                app.index_encoding(),
                app.algorithm(),
                app.k(),
                app.scorer_params(),
                app.weighted(),
                safe,
                runs,
                output
            );
            if (app.is_wand_compressed()) {
                if (quantized) {
                    std::apply(perftest<Index, wand_uniform_index_quantized>, params);
                } else {
                    std::apply(perftest<Index, wand_uniform_index>, params);
                }
            } else {
                std::apply(perftest<Index, wand_raw_index>, params);
            }
        }
    );
}
