#include <iostream>
#include <optional>
#include <string>
#include <vector>

#include <CLI/CLI.hpp>
#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>
#include <mio/mmap.hpp>
#include <range/v3/view/enumerate.hpp>
#include <spdlog/sinks/null_sink.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/spdlog.h>

#include "accumulator/lazy_accumulator.hpp"
#include "cursor/block_max_scored_cursor.hpp"
#include "cursor/cursor.hpp"
#include "cursor/max_scored_cursor.hpp"
#include "cursor/scored_cursor.hpp"
#include "index_types.hpp"
#include "mappable/mapper.hpp"
#include "query/algorithm.hpp"
#include "query/queries.hpp"
#include "scorer/scorer.hpp"
#include "timer.hpp"
#include "util/do_not_optimize_away.hpp"
#include "util/util.hpp"
#include "wand_data_compressed.hpp"
#include "wand_data_raw.hpp"

using namespace pisa;
using ranges::view::enumerate;

namespace pisa {

void extract_times(QueryExecutor fn,
                   std::vector<Query> const &queries,
                   std::string const &index_type,
                   std::string const &query_type,
                   size_t runs,
                   std::ostream &os)
{
    std::vector<std::size_t> times(runs);
    for (auto &&[qid, query] : enumerate(queries)) {
        do_not_optimize_away(fn(query));
        std::generate(times.begin(), times.end(), [&fn, &q = query]() {
            return run_with_timer<std::chrono::microseconds>(
                       [&]() { do_not_optimize_away(fn(q)); })
                .count();
        });
        auto mean = std::accumulate(times.begin(), times.end(), std::size_t{0}, std::plus<>()) / runs;
        os << fmt::format("{}\t{}\n", query.id.value_or(std::to_string(qid)), mean);
    }
}

void op_perftest(QueryExecutor query_func,
                 std::vector<Query> const &queries,
                 std::string const &index_type,
                 std::string const &query_type,
                 size_t runs)
{

    std::vector<double> query_times;

    for (size_t run = 0; run <= runs; ++run) {
        for (auto const &query : queries) {
            auto usecs = run_with_timer<std::chrono::microseconds>([&]() {
                auto result = query_func(query);
                do_not_optimize_away(result);
            });
            if (run != 0) { // first run is not timed
                query_times.push_back(usecs.count());
            }
        }
    }

    if (false) {
        for (auto t : query_times) {
            std::cout << (t / 1000) << std::endl;
        }
    } else {
        std::sort(query_times.begin(), query_times.end());
        double avg =
            std::accumulate(query_times.begin(), query_times.end(), double()) / query_times.size();
        double q50 = query_times[query_times.size() / 2];
        double q90 = query_times[90 * query_times.size() / 100];
        double q95 = query_times[95 * query_times.size() / 100];

        spdlog::info("---- {} {}", index_type, query_type);
        spdlog::info("Mean: {}", avg);
        spdlog::info("50% quantile: {}", q50);
        spdlog::info("90% quantile: {}", q90);
        spdlog::info("95% quantile: {}", q95);

        stats_line()("type", index_type)("query", query_type)("avg", avg)("q50", q50)("q90", q90)(
            "q95", q95);
    }
}

} // namespace pisa
