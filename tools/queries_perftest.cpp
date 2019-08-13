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

namespace pisa {

using wand_raw_index = wand_data<wand_data_raw>;
using wand_uniform_index = wand_data<wand_data_compressed>;

void extract_times(std::function<std::uint64_t(Query)> fn,
                   std::vector<Query> const &queries,
                   std::string const &index_type,
                   std::string const &query_type,
                   size_t runs,
                   std::ostream &os);

void op_perftest(std::function<std::uint64_t(Query)> query_func,
                 std::vector<Query> const &queries,
                 std::string const &index_type,
                 std::string const &query_type,
                 size_t runs);

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

    auto run_with_scorer = [&](auto scorer) {
        spdlog::info("Performing {} queries", type);
        spdlog::info("K: {}", k);

        for (auto &&t : query_types) {
            spdlog::info("Query type: {}", t);
            std::function<std::uint64_t(Query)> query_fun;
            if (t == "and") {
                query_fun = and_executor(index);
            } else if (t == "or") {
                query_fun = or_executor(index, false);
            } else if (t == "or_freq") {
                query_fun = or_executor(index, true);
            } else if (t == "wand" && wand_data_filename) {
                query_fun = wand_executor(index, wdata, scorer, k);
            } else if (t == "block_max_wand" && wand_data_filename) {
                query_fun = block_max_wand_executor(index, wdata, scorer, k);
            } else if (t == "block_max_maxscore" && wand_data_filename) {
                query_fun = block_max_maxscore_executor(index, wdata, scorer, k);
            } else if (t == "ranked_and" && wand_data_filename) {
                query_fun = ranked_or_executor(index, scorer, k);
            } else if (t == "block_max_ranked_and" && wand_data_filename) {
                query_fun = block_max_ranked_and_executor(index, wdata, scorer, k);
            } else if (t == "ranked_or" && wand_data_filename) {
                query_fun = ranked_or_executor(index, scorer, k);
            } else if (t == "maxscore" && wand_data_filename) {
                query_fun = maxscore_executor(index, wdata, scorer, k);
            } else if (t == "ranked_or_taat" && wand_data_filename) {
                SimpleAccumulator accumulator(index.num_docs());
                ranked_or_taat_query ranked_or_taat_q(k);
                query_fun = [&, ranked_or_taat_q, accumulator](Query query) mutable {
                    auto cursors = make_scored_cursors(index, scorer, query);
                    return ranked_or_taat_q(gsl::make_span(cursors), index.num_docs(), accumulator);
                };
            } else if (t == "ranked_or_taat_lazy" && wand_data_filename) {
                LazyAccumulator<4> accumulator(index.num_docs());
                ranked_or_taat_query ranked_or_taat_q(k);
                query_fun = [&, ranked_or_taat_q, accumulator](Query query) mutable {
                    auto cursors = make_scored_cursors(index, scorer, query);
                    return ranked_or_taat_q(gsl::make_span(cursors), index.num_docs(), accumulator);
                };
            } else {
                spdlog::error("Unsupported query type: {}", t);
                break;
            }
            if (extract) {
                extract_times(query_fun, queries, type, t, 2, std::cout);
            } else {
                op_perftest(query_fun, queries, type, t, 2);
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
