#include <optional>
#include <string>
#include <vector>

#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>
#include <mio/mmap.hpp>
#include <spdlog/spdlog.h>

#include "cursor/block_max_scored_cursor.hpp"
#include "cursor/cursor.hpp"
#include "cursor/max_scored_cursor.hpp"
#include "cursor/scored_cursor.hpp"
#include "index_types.hpp"
#include "query/algorithm.hpp"
#include "query/queries.hpp"
#include "wand_data.hpp"
#include "wand_data_compressed.hpp"
#include "wand_data_raw.hpp"

#include "queries/def.hpp"

namespace pisa {

using wand_raw_index = wand_data<wand_data_raw>;
using wand_uniform_index = wand_data<wand_data_compressed>;

template <typename IndexType, typename WandType>
void perftest(std::string const &index_filename,
              std::optional<std::string> const &wand_data_filename,
              std::vector<Query> const &queries,
              std::optional<std::string> const &thresholds_filename,
              std::string const &type,
              std::string const &query_type,
              std::uint64_t k,
              std::string const &scorer_name,
              bool extract)
{
    IndexType index;
    spdlog::info("Loading index from {}", index_filename);
    mio::mmap_source m(index_filename.c_str());
    mapper::map(index, m);

    spdlog::info("Warming up posting lists");
    std::unordered_set<term_id_type> warmed_up;
    for (auto const &q : queries) {
        for (auto t : q.terms) {
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
        if(error){
            std::cerr << "error mapping file: " << error.message() << ", exiting..." << std::endl;
            throw std::runtime_error("Error opening file");
        }
        mapper::map(wdata, md, mapper::map_flags::warmup);
    }

    std::vector<float> thresholds;
    if (thresholds_filename) {
        std::string t;
        std::ifstream tin(*thresholds_filename);
        while (std::getline(tin, t)) {
            thresholds.push_back(std::stof(t));
        }
    }

    auto run_with_scorer = [&](auto scorer) -> void {
        spdlog::info("Performing {} queries", type);
        spdlog::info("K: {}", k);

        for (auto &&t : query_types) {
            spdlog::info("Query type: {}", t);
            QueryBenchmarkLoop<IndexType, WandType, decltype(scorer)> qloop;
            if (t == "and") {
                qloop = query_benchmark_loop<IndexType, and_query, WandType, decltype(scorer)>;
            } else if (t == "or") {
                qloop =
                    query_benchmark_loop<IndexType, or_query<false>, WandType, decltype(scorer)>;
            } else if (t == "or_freq") {
                qloop = query_benchmark_loop<IndexType, or_query<true>, WandType, decltype(scorer)>;
            } else if (t == "ranked_or") {
                qloop =
                    query_benchmark_loop<IndexType, ranked_or_query, WandType, decltype(scorer)>;
            } else if (t == "ranked_and") {
                qloop =
                    query_benchmark_loop<IndexType, ranked_and_query, WandType, decltype(scorer)>;
            } else if (t == "wand") {
                qloop = query_benchmark_loop<IndexType, wand_query, WandType, decltype(scorer)>;
            } else if (t == "maxscore") {
                qloop = query_benchmark_loop<IndexType, maxscore_query, WandType, decltype(scorer)>;
            } else if (t == "block_max_wand") {
                qloop = query_benchmark_loop<IndexType,
                                             block_max_wand_query,
                                             WandType,
                                             decltype(scorer)>;
            } else if (t == "block_max_maxscore") {
                qloop = query_benchmark_loop<IndexType,
                                             block_max_maxscore_query,
                                             WandType,
                                             decltype(scorer)>;
            } else if (t == "block_max_ranked_and") {
                qloop = query_benchmark_loop<IndexType,
                                             block_max_ranked_and_query,
                                             WandType,
                                             decltype(scorer)>;
            } else if (t == "ranked_or_taat") {
                qloop = query_benchmark_loop<IndexType,
                                             ranked_or_taat_query,
                                             WandType,
                                             decltype(scorer)>;
            } else if (t == "ranked_or_taat_lazy") {
                qloop =
                    query_benchmark_loop<IndexType, LazyAccumulator<4>, WandType, decltype(scorer)>;
            } else {
                spdlog::error("Unsupported query type: {}", t);
                break;
            }

            auto times = qloop(index, wdata, scorer, queries, k, 5);

            if (extract) {
                for (std::size_t qidx = 0; qidx < times.size(); ++qidx) {
                    std::cout << fmt::format("{}\t{}\n",
                                             queries[qidx].id.value_or(std::to_string(qidx)),
                                             times[qidx].count());
                }
            } else {
                std::vector<double> query_times(times.size());
                std::transform(times.begin(), times.end(), query_times.begin(), [](auto micros) {
                    return micros.count();
                });
                std::sort(query_times.begin(), query_times.end());
                double avg = std::accumulate(query_times.begin(), query_times.end(), double())
                             / query_times.size();
                double q50 = query_times[query_times.size() / 2];
                double q90 = query_times[90 * query_times.size() / 100];
                double q95 = query_times[95 * query_times.size() / 100];

                spdlog::info("---- {} {}", t, query_type);
                spdlog::info("Mean: {}", avg);
                spdlog::info("50% quantile: {}", q50);
                spdlog::info("90% quantile: {}", q90);
                spdlog::info("95% quantile: {}", q95);

                stats_line()("type", t)("query",
                                        query_type)("avg", avg)("q50", q50)("q90", q90)("q95", q95);
            }
        }
    };
    with_scorer(scorer_name, wdata, run_with_scorer);
}

void perftest(std::string const &index_filename,
              std::optional<std::string> const &wand_data_filename,
              std::vector<Query> const &queries,
              std::optional<std::string> const &thresholds_filename,
              std::string const &type,
              std::string const &query_type,
              std::uint64_t k,
              std::string const &scorer_name,
              bool extract,
              bool compressed)
{
    /**/
    if (false) {
#define LOOP_BODY(R, DATA, T)                                                          \
    }                                                                                  \
    else if (type == BOOST_PP_STRINGIZE(T))                                            \
    {                                                                                  \
        if (compressed) {                                                              \
            perftest<BOOST_PP_CAT(T, _index), wand_uniform_index>(index_filename,      \
                                                                  wand_data_filename,  \
                                                                  queries,             \
                                                                  thresholds_filename, \
                                                                  type,                \
                                                                  query_type,          \
                                                                  k,                   \
                                                                  scorer_name,         \
                                                                  extract);            \
        } else {                                                                       \
            perftest<BOOST_PP_CAT(T, _index), wand_raw_index>(index_filename,          \
                                                              wand_data_filename,      \
                                                              queries,                 \
                                                              thresholds_filename,     \
                                                              type,                    \
                                                              query_type,              \
                                                              k,                       \
                                                              scorer_name,             \
                                                              extract);                \
        }                                                                              \
        /**/

        BOOST_PP_SEQ_FOR_EACH(LOOP_BODY, _, PISA_INDEX_TYPES);
#undef LOOP_BODY

    } else {
        spdlog::error("Unknown type {}", type);
    }
}

} // namespace pisa
