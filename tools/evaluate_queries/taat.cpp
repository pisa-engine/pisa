#include "def.hpp"
#include "query/algorithm/ranked_or_taat_query.hpp"

namespace pisa {

#define PISA_SIMPLE_ACC_QUERY_LOOP(SCORER, INDEX, WAND)                                \
    template <>                                                                        \
    auto query_loop<BOOST_PP_CAT(INDEX, _index),                                       \
                    pisa::ranked_or_taat_query,                                        \
                    wand_data<WAND>,                                                   \
                    SCORER<wand_data<WAND>>>(BOOST_PP_CAT(INDEX, _index) const &index, \
                                             wand_data<WAND> const &,                  \
                                             SCORER<wand_data<WAND>> scorer,           \
                                             std::vector<Query> const &queries,        \
                                             int k)                                    \
        ->std::vector<ResultVector>                                                    \
    {                                                                                  \
        std::vector<ResultVector> results(queries.size());                             \
        SimpleAccumulator accumulator(index.num_docs());                               \
        auto run = ranked_or_taat_query(k);                                            \
        for (std::size_t qidx = 0; qidx < queries.size(); ++qidx) {                    \
            auto query = queries[qidx];                                                \
            auto cursors = make_scored_cursors(index, scorer, query);                  \
            run(gsl::make_span(cursors), index.num_docs(), accumulator);               \
            results[qidx] = run.topk();                                                \
        }                                                                              \
        return results;                                                                \
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

#define PISA_LAZY_ACC_QUERY_LOOP(SCORER, INDEX, WAND)                                  \
    template <>                                                                        \
    auto query_loop<BOOST_PP_CAT(INDEX, _index),                                       \
                    LazyAccumulator<4>,                                                \
                    wand_data<WAND>,                                                   \
                    SCORER<wand_data<WAND>>>(BOOST_PP_CAT(INDEX, _index) const &index, \
                                             wand_data<WAND> const &,                  \
                                             SCORER<wand_data<WAND>> scorer,           \
                                             std::vector<Query> const &queries,        \
                                             int k)                                    \
        ->std::vector<ResultVector>                                                    \
    {                                                                                  \
        std::vector<ResultVector> results(queries.size());                             \
        LazyAccumulator<4> accumulator(index.num_docs());                              \
        auto run = ranked_or_taat_query(k);                                            \
        for (std::size_t qidx = 0; qidx < queries.size(); ++qidx) {                    \
            auto query = queries[qidx];                                                \
            auto cursors = make_scored_cursors(index, scorer, query);                  \
            run(gsl::make_span(cursors), index.num_docs(), accumulator);               \
            results[qidx] = run.topk();                                                \
        }                                                                              \
        return results;                                                                \
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
