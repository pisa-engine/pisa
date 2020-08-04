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
#include "cursor/range_block_max_scored_cursor.hpp"
#include "cursor/max_scored_cursor.hpp"
#include "cursor/scored_cursor.hpp"
#include "index_types.hpp"
#include "mappable/mapper.hpp"
#include "memory_source.hpp"
#include "query/algorithm.hpp"
#include "scorer/scorer.hpp"
#include "timer.hpp"
#include "topk_queue.hpp"
#include "util/util.hpp"
#include "wand_data_compressed.hpp"
#include "wand_data_raw.hpp"
#include "query/live_block_computation.hpp"

using namespace pisa;
using ranges::views::enumerate;



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

    std::vector<Threshold> thresholds(queries.size(), 0.0);
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

    auto runs = 2;

    std::vector<double> query_times;
    spdlog::info("Safe: {}", safe);

    size_t blocks_num = ceil_div(index.num_docs(), 32);
    for (size_t run = 0; run <= runs; ++run) {
        size_t idx = 0;
        for (auto const& query: queries) {
            auto usecs = run_with_timer<std::chrono::microseconds>([&]() {

              for (auto t: query.terms) {
                auto docs_enum = index[t];
                if(docs_enum.size() < 262144){
                  auto s = scorer->term_scorer(t);
                  auto tmp = wand_data_range<32, 0>::compute_block_max_scores(docs_enum, s, blocks_num);
                  do_not_optimize_away(tmp);
                }
              }

            });
            if (run != 0) {  // first run is not timed
                query_times.push_back(usecs.count());
            }
            idx += 1;
        }
    }

        std::sort(query_times.begin(), query_times.end());
        double avg =
            std::accumulate(query_times.begin(), query_times.end(), double()) / query_times.size();
        double q50 = query_times[query_times.size() / 2];
        double q90 = query_times[90 * query_times.size() / 100];
        double q95 = query_times[95 * query_times.size() / 100];
        double q99 = query_times[99 * query_times.size() / 100];

        spdlog::info("Mean: {}", avg);
        spdlog::info("50% quantile: {}", q50);
        spdlog::info("90% quantile: {}", q90);
        spdlog::info("95% quantile: {}", q95);
        spdlog::info("99% quantile: {}", q99);



}

using wand_raw_index = wand_data<wand_data_raw>;
using wand_uniform_index = wand_data<wand_data_compressed<>>;
using wand_uniform_index_quantized = wand_data<wand_data_compressed<PayloadType::Quantized>>;

int main(int argc, const char** argv)
{
    bool extract = false;
    bool silent = false;
    bool safe = false;
    bool quantized = false;

    App<arg::Index,
        arg::WandData<arg::WandMode::Optional>,
        arg::Query<arg::QueryMode::Ranked>,
        arg::Algorithm,
        arg::Scorer,
        arg::Thresholds>
        app{"Benchmarks queries on a given index."};
    app.add_flag("--quantized", quantized, "Quantized scores");
    app.add_flag("--extract", extract, "Extract individual query times");
    app.add_flag("--silent", silent, "Suppress logging");
    app.add_flag("--safe", safe, "Rerun if not enough results with pruning.")
        ->needs(app.thresholds_option());
    CLI11_PARSE(app, argc, argv);

    if (silent) {
        spdlog::set_default_logger(spdlog::create<spdlog::sinks::null_sink_mt>("stderr"));
    } else {
        spdlog::set_default_logger(spdlog::stderr_color_mt("stderr"));
    }
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