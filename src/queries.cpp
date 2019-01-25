#include <iostream>
#include <optional>

#include "boost/algorithm/string/classification.hpp"
#include "boost/algorithm/string/split.hpp"
#include "mio/mmap.hpp"
#include "spdlog/spdlog.h"

#include "succinct/mapper.hpp"

#include "index_types.hpp"
#include "query/queries.hpp"
#include "timer.hpp"
#include "util/util.hpp"
#include "wand_data_compressed.hpp"
#include "wand_data_raw.hpp"

#include "CLI/CLI.hpp"

using namespace pisa;

template <typename Functor>
void op_perftest(Functor query_func, // XXX!!!
                 std::vector<pisa::term_id_vec> const &queries,
                 std::string const &index_type,
                 std::string const &query_type,
                 size_t runs) {

    std::vector<double> query_times;

    for (size_t run = 0; run <= runs; ++run) {
        for (auto const &query : queries) {
            auto usecs = run_with_timer<std::chrono::microseconds>([&]() {
                uint64_t result = query_func(query);
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

template <typename IndexType, typename WandType>
void perftest(const std::string &index_filename,
              const std::optional<std::string> &wand_data_filename,
              const std::vector<term_id_vec> &queries,
              const std::optional<std::string> &thresholds_filename,
              std::string const &type,
              std::string const &query_type,
              uint64_t k)
{
    IndexType index;
    spdlog::info("Loading index from {}", index_filename);
    mio::mmap_source m(index_filename.c_str());
    mapper::map(index, m);

    spdlog::info("Warming up posting lists");
    std::unordered_set<term_id_type> warmed_up;
    for (auto const &q : queries) {
        for (auto t : q) {
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

    spdlog::info("Performing {} queries", type);
    spdlog::info("K: {}", k);

    for (auto &&t : query_types) {
        spdlog::info("Query type: {}", t);
        std::function<uint64_t(term_id_vec)> query_fun;
        if (t == "and") {
            query_fun = and_query<IndexType, false>(index);
        } else if (t == "and_freq") {
            query_fun = and_query<IndexType, true>(index);
        } else if (t == "or") {
            query_fun = or_query<IndexType, false>(index);
        } else if (t == "or_freq") {
            query_fun = or_query<IndexType, true>(index);
        } else if (t == "wand" && wand_data_filename) {
            query_fun = wand_query<IndexType, WandType>(index, wdata, k);
        } else if (t == "block_max_wand" && wand_data_filename) {
            query_fun =block_max_wand_query<IndexType, WandType>(index, wdata, k);
        } else if (t == "block_max_maxscore" && wand_data_filename) {
            query_fun = block_max_maxscore_query<IndexType, WandType>(index, wdata, k);
        }  else if (t == "ranked_or" && wand_data_filename) {
            query_fun = ranked_or_query<IndexType, WandType>(index, wdata, k);
        } else if (t == "maxscore" && wand_data_filename) {
            query_fun = maxscore_query<IndexType, WandType>(index, wdata, k);
        } else if (t == "ranked_or_taat" && wand_data_filename) {
            query_fun = pisa::make_ranked_or_taat_query<pisa::Simple_Accumulator>(index, wdata, k);
        } else {
            spdlog::error("Unsupported query type: {}", t);
            break;
        }
        op_perftest(query_fun, queries, type, t, 2);
    }
}

typedef wand_data<bm25, wand_data_raw<bm25>> wand_raw_index;
typedef wand_data<bm25, wand_data_compressed<bm25, uniform_score_compressor>> wand_uniform_index;

int main(int argc, const char **argv) {
    std::string type;
    std::string query_type;
    std::string index_filename;
    std::optional<std::string> wand_data_filename;
    std::optional<std::string> query_filename;
    std::optional<std::string> thresholds_filename;
    uint64_t k = configuration::get().k;
    bool compressed = false;

    CLI::App app{"queries - a tool for performing queries on an index."};
    app.add_option("-t,--type", type, "Index type")->required();
    app.add_option("-a,--algorithm", query_type, "Query algorithm")->required();
    app.add_option("-i,--index", index_filename, "Collection basename")->required();
    app.add_option("-w,--wand", wand_data_filename, "Wand data filename");
    app.add_option("-q,--query", query_filename, "Queries filename");
    app.add_flag("--compressed-wand", compressed, "Compressed wand input file");
    app.add_option("-k", k, "k value");
    app.add_option("-T,--thresholds", thresholds_filename, "k value");
    CLI11_PARSE(app, argc, argv);

    std::vector<term_id_vec> queries;
    term_id_vec q;
    if (query_filename) {
        std::filebuf fb;
        if (fb.open(*query_filename, std::ios::in)) {
            std::istream is(&fb);
            while (read_query(q, is))
                queries.push_back(q);
        }
    } else {
        while (read_query(q))
            queries.push_back(q);
    }

    /**/
    if (false) {
#define LOOP_BODY(R, DATA, T)                                                          \
    }                                                                                  \
    else if (type == BOOST_PP_STRINGIZE(T)) {                                          \
        if (compressed) {                                                              \
            perftest<BOOST_PP_CAT(T, _index), wand_uniform_index>(index_filename,      \
                                                                  wand_data_filename,  \
                                                                  queries,             \
                                                                  thresholds_filename, \
                                                                  type,                \
                                                                  query_type,          \
                                                                  k);                  \
        } else {                                                                       \
            perftest<BOOST_PP_CAT(T, _index), wand_raw_index>(index_filename,          \
                                                              wand_data_filename,      \
                                                              queries,                 \
                                                              thresholds_filename,     \
                                                              type,                    \
                                                              query_type,              \
                                                              k);                      \
        }                                                                              \
        /**/

        BOOST_PP_SEQ_FOR_EACH(LOOP_BODY, _, DS2I_INDEX_TYPES);
#undef LOOP_BODY

    } else {
        spdlog::error("Unknown type {}", type);
    }
}
