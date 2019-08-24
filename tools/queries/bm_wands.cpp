#include "def.hpp"
#include "query/algorithm/block_max_maxscore_query.hpp"
#include "query/algorithm/block_max_ranked_and_query.hpp"
#include "query/algorithm/block_max_wand_query.hpp"
#include "timer.hpp"
#include "util/do_not_optimize_away.hpp"

namespace pisa {

#define PISA_BMW_LOOP(SCORER, INDEX, WAND)                                                       \
    template <>                                                                                  \
    auto query_benchmark_loop<BOOST_PP_CAT(INDEX, _index),                                       \
                              pisa::block_max_wand_query,                                        \
                              wand_data<WAND>,                                                   \
                              SCORER<wand_data<WAND>>>(BOOST_PP_CAT(INDEX, _index) const &index, \
                                                       wand_data<WAND> const &wdata,             \
                                                       SCORER<wand_data<WAND>> scorer,           \
                                                       std::vector<Query> const &queries,        \
                                                       int k,                                    \
                                                       int runs)                                 \
        ->std::vector<std::chrono::microseconds>                                                 \
    {                                                                                            \
        auto execute = block_max_wand_query(k);                                                  \
        std::vector<std::chrono::microseconds> query_times;                                      \
        for (auto const &query : queries) {                                                      \
            std::chrono::microseconds min_time;                                                  \
            for (std::size_t run = 0; run <= runs; ++run) {                                      \
                auto usecs = run_with_timer<std::chrono::microseconds>([&]() {                   \
                    auto cursors = make_block_max_scored_cursors(index, wdata, scorer, query);   \
                    auto result = execute(gsl::make_span(cursors), index.num_docs());            \
                    do_not_optimize_away(result);                                                \
                });                                                                              \
                if (run != 0) {                                                                  \
                    min_time = std::min(min_time, usecs);                                        \
                } else {                                                                         \
                    min_time = usecs;                                                            \
                }                                                                                \
            }                                                                                    \
            query_times.push_back(min_time);                                                     \
        }                                                                                        \
        return query_times;                                                                      \
    }

#define LOOP_BODY(R, DATA, T)                           \
    PISA_BMW_LOOP(bm25, T, wand_data_raw)        \
    PISA_BMW_LOOP(dph, T, wand_data_raw)         \
    PISA_BMW_LOOP(pl2, T, wand_data_raw)         \
    PISA_BMW_LOOP(qld, T, wand_data_raw)         \
    PISA_BMW_LOOP(bm25, T, wand_data_compressed) \
    PISA_BMW_LOOP(dph, T, wand_data_compressed)  \
    PISA_BMW_LOOP(pl2, T, wand_data_compressed)  \
    PISA_BMW_LOOP(qld, T, wand_data_compressed)  \
/**/
BOOST_PP_SEQ_FOR_EACH(LOOP_BODY, _, PISA_INDEX_TYPES);
#undef LOOP_BODY
#undef PISA_BMW_LOOP

#define PISA_BMM_LOOP(SCORER, INDEX, WAND)                                                       \
    template <>                                                                                  \
    auto query_benchmark_loop<BOOST_PP_CAT(INDEX, _index),                                       \
                              pisa::block_max_maxscore_query,                                    \
                              wand_data<WAND>,                                                   \
                              SCORER<wand_data<WAND>>>(BOOST_PP_CAT(INDEX, _index) const &index, \
                                                       wand_data<WAND> const &wdata,             \
                                                       SCORER<wand_data<WAND>> scorer,           \
                                                       std::vector<Query> const &queries,        \
                                                       int k,                                    \
                                                       int runs)                                 \
        ->std::vector<std::chrono::microseconds>                                                 \
    {                                                                                            \
        auto execute = block_max_maxscore_query(k);                                              \
        std::vector<std::chrono::microseconds> query_times;                                      \
        for (auto const &query : queries) {                                                      \
            std::chrono::microseconds min_time;                                                  \
            for (std::size_t run = 0; run <= runs; ++run) {                                      \
                auto usecs = run_with_timer<std::chrono::microseconds>([&]() {                   \
                    auto cursors = make_block_max_scored_cursors(index, wdata, scorer, query);   \
                    auto result = execute(gsl::make_span(cursors), index.num_docs());            \
                    do_not_optimize_away(result);                                                \
                });                                                                              \
                if (run != 0) {                                                                  \
                    min_time = std::min(min_time, usecs);                                        \
                } else {                                                                         \
                    min_time = usecs;                                                            \
                }                                                                                \
            }                                                                                    \
            query_times.push_back(min_time);                                                     \
        }                                                                                        \
        return query_times;                                                                      \
    }

#define LOOP_BODY(R, DATA, T)                    \
    PISA_BMM_LOOP(bm25, T, wand_data_raw)        \
    PISA_BMM_LOOP(dph, T, wand_data_raw)         \
    PISA_BMM_LOOP(pl2, T, wand_data_raw)         \
    PISA_BMM_LOOP(qld, T, wand_data_raw)         \
    PISA_BMM_LOOP(bm25, T, wand_data_compressed) \
    PISA_BMM_LOOP(dph, T, wand_data_compressed)  \
    PISA_BMM_LOOP(pl2, T, wand_data_compressed)  \
    PISA_BMM_LOOP(qld, T, wand_data_compressed)  \
/**/
BOOST_PP_SEQ_FOR_EACH(LOOP_BODY, _, PISA_INDEX_TYPES);
#undef LOOP_BODY
#undef PISA_BMM_LOOP

#define PISA_BMA_LOOP(SCORER, INDEX, WAND)                                                       \
    template <>                                                                                  \
    auto query_benchmark_loop<BOOST_PP_CAT(INDEX, _index),                                       \
                              pisa::block_max_ranked_and_query,                                  \
                              wand_data<WAND>,                                                   \
                              SCORER<wand_data<WAND>>>(BOOST_PP_CAT(INDEX, _index) const &index, \
                                                       wand_data<WAND> const &wdata,             \
                                                       SCORER<wand_data<WAND>> scorer,           \
                                                       std::vector<Query> const &queries,        \
                                                       int k,                                    \
                                                       int runs)                                 \
        ->std::vector<std::chrono::microseconds>                                                 \
    {                                                                                            \
        auto execute = block_max_ranked_and_query(k);                                            \
        std::vector<std::chrono::microseconds> query_times;                                      \
        for (auto const &query : queries) {                                                      \
            std::chrono::microseconds min_time;                                                  \
            for (std::size_t run = 0; run <= runs; ++run) {                                      \
                auto usecs = run_with_timer<std::chrono::microseconds>([&]() {                   \
                    auto cursors = make_block_max_scored_cursors(index, wdata, scorer, query);   \
                    auto result = execute(gsl::make_span(cursors), index.num_docs());            \
                    do_not_optimize_away(result);                                                \
                });                                                                              \
                if (run != 0) {                                                                  \
                    min_time = std::min(min_time, usecs);                                        \
                } else {                                                                         \
                    min_time = usecs;                                                            \
                }                                                                                \
            }                                                                                    \
            query_times.push_back(min_time);                                                     \
        }                                                                                        \
        return query_times;                                                                      \
    }

#define LOOP_BODY(R, DATA, T)                    \
    PISA_BMA_LOOP(bm25, T, wand_data_raw)        \
    PISA_BMA_LOOP(dph, T, wand_data_raw)         \
    PISA_BMA_LOOP(pl2, T, wand_data_raw)         \
    PISA_BMA_LOOP(qld, T, wand_data_raw)         \
    PISA_BMA_LOOP(bm25, T, wand_data_compressed) \
    PISA_BMA_LOOP(dph, T, wand_data_compressed)  \
    PISA_BMA_LOOP(pl2, T, wand_data_compressed)  \
    PISA_BMA_LOOP(qld, T, wand_data_compressed)  \
/**/
BOOST_PP_SEQ_FOR_EACH(LOOP_BODY, _, PISA_INDEX_TYPES);
#undef LOOP_BODY
#undef PISA_BMA_LOOP

} // namespace pisa
