// Copyright 2025 PISA Developers
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <algorithm>
#include <fstream>
#include <iostream>
#include <numeric>
#include <optional>
#include <string>

#include <CLI/CLI.hpp>
#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>
#include <nlohmann/json.hpp>
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

class AggregationType {
  public:
    enum Value { None = 0, Min = 1, Mean = 2, Median = 3, Max = 4 };

    constexpr AggregationType(Value value) : m_value(value) {}
    constexpr operator Value() const { return m_value; }

    [[nodiscard]] auto to_string() const -> std::string {
        switch (m_value) {
        case None: return "none";
        case Min: return "min";
        case Mean: return "mean";
        case Median: return "median";
        case Max: return "max";
        }
        throw std::logic_error("Unknown AggregationType");
    }

  private:
    Value m_value;
};

struct QueryTimesSummary {
    AggregationType aggregation_type;
    double mean;
    double q50;
    double q90;
    double q95;
    double q99;

    [[nodiscard]] auto to_json() const -> nlohmann::json {
        return {
            {"query_aggregation", aggregation_type.to_string()},
            {"mean", mean},
            {"q50", q50},
            {"q90", q90},
            {"q95", q95},
            {"q99", q99}
        };
    }
};

struct QueryTimes {
    std::vector<std::vector<std::size_t>> values;
    std::size_t corrective_rerun_count;

    auto aggregate(AggregationType aggregation_type) const -> std::vector<std::size_t> {
        std::vector<std::size_t> aggregated_query_times;
        if (aggregation_type == AggregationType::None) {
            for (auto const& times_per_run: values) {
                for (auto t: times_per_run) {
                    aggregated_query_times.push_back(t);
                }
            }
        } else if (aggregation_type == AggregationType::Min) {
            for (auto const& times_per_run: values) {
                aggregated_query_times.push_back(
                    *std::min_element(times_per_run.begin(), times_per_run.end())
                );
            }
        } else if (aggregation_type == AggregationType::Mean) {
            for (auto const& times_per_run: values) {
                double sum = std::accumulate(times_per_run.begin(), times_per_run.end(), double());
                double mean = sum / times_per_run.size();
                aggregated_query_times.push_back(mean);
            }
        } else if (aggregation_type == AggregationType::Median) {
            for (auto const& times_per_run: values) {
                auto sorted_times = times_per_run;
                std::sort(sorted_times.begin(), sorted_times.end());
                std::size_t sample_count = sorted_times.size();
                double median = 0;
                if (sample_count % 2 == 1) {
                    median = sorted_times[sample_count / 2];
                } else {
                    median =
                        (sorted_times[sample_count / 2] + sorted_times[sample_count / 2 - 1]) / 2;
                }
                aggregated_query_times.push_back(median);
            }
        } else if (aggregation_type == AggregationType::Max) {
            for (auto const& times_per_run: values) {
                aggregated_query_times.push_back(
                    *std::max_element(times_per_run.begin(), times_per_run.end())
                );
            }
        }
        std::sort(aggregated_query_times.begin(), aggregated_query_times.end());
        return aggregated_query_times;
    }

    auto summarize(AggregationType agg_type) const -> QueryTimesSummary {
        auto aggregated_times = aggregate(agg_type);

        double mean = std::accumulate(aggregated_times.begin(), aggregated_times.end(), double())
            / aggregated_times.size();
        double q50 = aggregated_times[aggregated_times.size() / 2];
        double q90 = aggregated_times[90 * aggregated_times.size() / 100];
        double q95 = aggregated_times[95 * aggregated_times.size() / 100];
        double q99 = aggregated_times[99 * aggregated_times.size() / 100];

        return {agg_type, mean, q50, q90, q95, q99};
    }
};

template <typename Fn>
auto extract_times(
    Fn query_func,
    std::vector<Query> const& queries,
    std::vector<Score> const& thresholds,
    size_t runs,
    std::uint64_t k,
    bool safe
) -> QueryTimes {
    QueryTimes query_times{
        std::vector<std::vector<std::size_t>>(queries.size(), std::vector<std::size_t>(runs)), 0
    };

    // Note: each query is measured once per run, so the set of queries is
    // measured independently in each run.
    for (size_t run = 0; run <= runs; ++run) {
        for (auto&& [query_idx, query]: enumerate(queries)) {
            auto usecs = run_with_timer<std::chrono::microseconds>([&]() {
                uint64_t result = query_func(query, thresholds[query_idx]);
                if (safe && result < k) {
                    query_times.corrective_rerun_count += 1;
                    result = query_func(query, 0);
                }
                do_not_optimize_away(result);
            });
            if (run != 0) {  // first run is not timed
                query_times.values[query_idx][run - 1] = usecs.count();
            }
        }
    }

    return query_times;
}

void print_summary(
    QueryTimes const& query_times,
    std::string const& index_type,
    std::string const& query_type,
    size_t runs,
    std::uint64_t k,
    bool safe
) {
    nlohmann::json summary;
    summary["encoding"] = index_type;
    summary["algorithm"] = query_type;
    summary["runs"] = runs;
    summary["k"] = k;
    summary["safe"] = safe;
    summary["corrective_reruns"] = query_times.corrective_rerun_count;
    summary["times"] = nlohmann::json::array();

    summary["times"].push_back(query_times.summarize(AggregationType::None).to_json());
    summary["times"].push_back(query_times.summarize(AggregationType::Min).to_json());
    summary["times"].push_back(query_times.summarize(AggregationType::Mean).to_json());
    summary["times"].push_back(query_times.summarize(AggregationType::Median).to_json());
    summary["times"].push_back(query_times.summarize(AggregationType::Max).to_json());
    std::cout << summary.dump(2) << "\n";
}

void print_times(
    QueryTimes const& query_times,
    std::vector<Query> const& queries,
    std::string const& query_type,
    std::ostream& output_stream
) {
    output_stream << "algorithm\tqid\trun\tusec\n";
    for (auto&& [query_idx, query]: enumerate(queries)) {
        for (auto&& [run_idx, time]: enumerate(query_times.values[query_idx])) {
            output_stream << fmt::format(
                "{}\t{}\t{}\t{}\n",
                query_type,
                query.id().value_or(std::to_string(query_idx)),
                run_idx + 1,
                time
            );
        }
    }
}

auto open_output_file(std::optional<std::string> const& output_path) -> std::optional<std::ofstream> {
    if (!output_path) {
        return std::nullopt;
    }

    std::ofstream out(*output_path);
    if (!out.is_open()) {
        const auto err_msg = fmt::format("Failed to open output file: {}.", *output_path);
        throw std::runtime_error(err_msg);
    }

    return out;
}

template <typename IndexType, typename WandType>
void perftest(
    IndexType const* index_ptr,
    const std::optional<std::string>& wand_data_filename,
    const std::vector<Query>& queries,
    const std::optional<std::string>& thresholds_filename,
    std::string const& type,
    std::vector<std::string> const& query_types,
    uint64_t k,
    const ScorerParams& scorer_params,
    const bool weighted,
    bool safe,
    std::size_t runs,
    std::ostream* output_stream
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

    for (std::size_t query_type_idx = 0; query_type_idx < query_types.size(); ++query_type_idx) {
        auto const& t = query_types[query_type_idx];
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
        auto query_times = extract_times(query_fun, queries, thresholds, runs, k, safe);
        print_summary(query_times, type, t, runs, k, safe);
        if (output_stream) {
            print_times(query_times, queries, t, *output_stream);
        }
    }
}

using wand_raw_index = wand_data<wand_data_raw>;
using wand_uniform_index = wand_data<wand_data_compressed<>>;
using wand_uniform_index_quantized = wand_data<wand_data_compressed<PayloadType::Quantized>>;

int main(int argc, const char** argv) {
    bool safe = false;
    bool quantized = false;
    std::size_t runs = 0;
    std::optional<std::string> output_path;

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
    app.add_option("-o,--output", output_path, "Output file for per-run query timing data");
    CLI11_PARSE(app, argc, argv);

    spdlog::set_default_logger(spdlog::stderr_color_mt("stderr"));
    spdlog::set_level(app.log_level());

    // Parse query types (algorithms)
    std::vector<std::string> query_types;
    boost::algorithm::split(query_types, app.algorithm(), boost::is_any_of(":"));

    // If required, attempt to open the output file
    std::optional<std::ofstream> output_file;
    std::ostream* output_stream = nullptr;
    try {
        output_file = open_output_file(output_path);
        if (output_file.has_value()) {
            output_stream = &*output_file;
            spdlog::info("Per-run query output will be saved to '{}'.", *output_path);
        }
    } catch (std::exception const& e) {
        spdlog::error("{}", e.what());
        return EXIT_FAILURE;
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
                query_types,
                app.k(),
                app.scorer_params(),
                app.weighted(),
                safe,
                runs,
                output_stream
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
