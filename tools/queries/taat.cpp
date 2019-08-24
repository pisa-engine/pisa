#include "def.hpp"
#include "query/algorithm/ranked_or_taat_query.hpp"
#include "timer.hpp"
#include "util/do_not_optimize_away.hpp"

namespace pisa {

#define PISA_SIMPLE_ACC_QUERY_LOOP(SCORER, INDEX, WAND)                                            \
    template <>                                                                                    \
    auto query_benchmark_loop<BOOST_PP_CAT(INDEX, _index),                                         \
                              pisa::ranked_or_taat_query,                                          \
                              wand_data<WAND>,                                                     \
                              SCORER<wand_data<WAND>>>(BOOST_PP_CAT(INDEX, _index) const &index,   \
                                                       wand_data<WAND> const &,                    \
                                                       SCORER<wand_data<WAND>> scorer,             \
                                                       std::vector<Query> const &queries,          \
                                                       int k,                                      \
                                                       int runs)                                   \
        ->std::vector<std::chrono::microseconds>                                                   \
    {                                                                                              \
        SimpleAccumulator accumulator(index.num_docs());                                           \
        auto execute = ranked_or_taat_query(k);                                                    \
        std::vector<std::chrono::microseconds> query_times;                                        \
        for (auto const &query : queries) {                                                        \
            std::chrono::microseconds min_time;                                                    \
            for (std::size_t run = 0; run <= runs; ++run) {                                        \
                auto usecs = run_with_timer<std::chrono::microseconds>([&]() {                     \
                    auto cursors = make_scored_cursors(index, scorer, query);                      \
                    auto result = execute(gsl::make_span(cursors), index.num_docs(), accumulator); \
                    do_not_optimize_away(result);                                                  \
                });                                                                                \
                if (run != 0) {                                                                    \
                    min_time = std::min(min_time, usecs);                                          \
                } else {                                                                           \
                    min_time = usecs;                                                              \
                }                                                                                  \
            }                                                                                      \
            query_times.push_back(min_time);                                                       \
        }                                                                                          \
        return query_times;                                                                        \
    }

#define LOOP_BODY(R, DATA, T)                                 \
    PISA_SIMPLE_ACC_QUERY_LOOP(bm25, T, wand_data_raw)        \
    PISA_SIMPLE_ACC_QUERY_LOOP(dph, T, wand_data_raw)         \
    PISA_SIMPLE_ACC_QUERY_LOOP(pl2, T, wand_data_raw)         \
    PISA_SIMPLE_ACC_QUERY_LOOP(qld, T, wand_data_raw)         \
    PISA_SIMPLE_ACC_QUERY_LOOP(bm25, T, wand_data_compressed) \
    PISA_SIMPLE_ACC_QUERY_LOOP(dph, T, wand_data_compressed)  \
    PISA_SIMPLE_ACC_QUERY_LOOP(pl2, T, wand_data_compressed)  \
    PISA_SIMPLE_ACC_QUERY_LOOP(qld, T, wand_data_compressed)  \
/**/
BOOST_PP_SEQ_FOR_EACH(LOOP_BODY, _, PISA_INDEX_TYPES);
#undef LOOP_BODY
#undef PISA_SIMPLE_ACC_QUERY_LOOP

#define PISA_LAZY_ACC_QUERY_LOOP(SCORER, INDEX, WAND)                                              \
    template <>                                                                                    \
    auto query_benchmark_loop<BOOST_PP_CAT(INDEX, _index),                                         \
                              LazyAccumulator<4>,                                                  \
                              wand_data<WAND>,                                                     \
                              SCORER<wand_data<WAND>>>(BOOST_PP_CAT(INDEX, _index) const &index,   \
                                                       wand_data<WAND> const &,                    \
                                                       SCORER<wand_data<WAND>> scorer,             \
                                                       std::vector<Query> const &queries,          \
                                                       int k,                                      \
                                                       int runs)                                   \
        ->std::vector<std::chrono::microseconds>                                                   \
    {                                                                                              \
        LazyAccumulator<4> accumulator(index.num_docs());                                          \
        auto execute = ranked_or_taat_query(k);                                                    \
        std::vector<std::chrono::microseconds> query_times;                                        \
        for (auto const &query : queries) {                                                        \
            std::chrono::microseconds min_time;                                                    \
            for (std::size_t run = 0; run <= runs; ++run) {                                        \
                auto usecs = run_with_timer<std::chrono::microseconds>([&]() {                     \
                    auto cursors = make_scored_cursors(index, scorer, query);                      \
                    auto result = execute(gsl::make_span(cursors), index.num_docs(), accumulator); \
                    do_not_optimize_away(result);                                                  \
                });                                                                                \
                if (run != 0) {                                                                    \
                    min_time = std::min(min_time, usecs);                                          \
                } else {                                                                           \
                    min_time = usecs;                                                              \
                }                                                                                  \
            }                                                                                      \
            query_times.push_back(min_time);                                                       \
        }                                                                                          \
        return query_times;                                                                        \
    }

#define LOOP_BODY(R, DATA, T)                               \
    PISA_LAZY_ACC_QUERY_LOOP(bm25, T, wand_data_raw)        \
    PISA_LAZY_ACC_QUERY_LOOP(dph, T, wand_data_raw)         \
    PISA_LAZY_ACC_QUERY_LOOP(pl2, T, wand_data_raw)         \
    PISA_LAZY_ACC_QUERY_LOOP(qld, T, wand_data_raw)         \
    PISA_LAZY_ACC_QUERY_LOOP(bm25, T, wand_data_compressed) \
    PISA_LAZY_ACC_QUERY_LOOP(dph, T, wand_data_compressed)  \
    PISA_LAZY_ACC_QUERY_LOOP(pl2, T, wand_data_compressed)  \
    PISA_LAZY_ACC_QUERY_LOOP(qld, T, wand_data_compressed)  \
/**/
BOOST_PP_SEQ_FOR_EACH(LOOP_BODY, _, PISA_INDEX_TYPES);
#undef LOOP_BODY
#undef PISA_LAZY_ACC_QUERY_LOOP

} // namespace pisa
