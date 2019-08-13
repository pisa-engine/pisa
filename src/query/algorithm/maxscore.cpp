#include "cursor/scored_cursor.hpp"
#include "index_types.hpp"
#include "macro.hpp"
#include "query/algorithm/maxscore_query.hpp"

namespace pisa {

#define PISA_MAXSCORE_EXECUTOR(SCORER, INDEX, WAND)                               \
    template QueryExecutor maxscore_executor(BOOST_PP_CAT(INDEX, _index) const &, \
                                             wand_data<WAND> const &,             \
                                             SCORER<wand_data<WAND>> const &,     \
                                             int);

#define LOOP_BODY(R, DATA, T)                                              \
    PISA_DAAT_MAX_ALGORITHM(maxscore_query, bm25, T, wand_data_raw)        \
    PISA_DAAT_MAX_ALGORITHM(maxscore_query, dph, T, wand_data_raw)         \
    PISA_DAAT_MAX_ALGORITHM(maxscore_query, pl2, T, wand_data_raw)         \
    PISA_DAAT_MAX_ALGORITHM(maxscore_query, qld, T, wand_data_raw)         \
    PISA_DAAT_MAX_ALGORITHM(maxscore_query, bm25, T, wand_data_compressed) \
    PISA_DAAT_MAX_ALGORITHM(maxscore_query, dph, T, wand_data_compressed)  \
    PISA_DAAT_MAX_ALGORITHM(maxscore_query, pl2, T, wand_data_compressed)  \
    PISA_DAAT_MAX_ALGORITHM(maxscore_query, qld, T, wand_data_compressed)  \
    PISA_MAXSCORE_EXECUTOR(bm25, T, wand_data_raw)                         \
    PISA_MAXSCORE_EXECUTOR(dph, T, wand_data_raw)                          \
    PISA_MAXSCORE_EXECUTOR(pl2, T, wand_data_raw)                          \
    PISA_MAXSCORE_EXECUTOR(qld, T, wand_data_raw)                          \
    PISA_MAXSCORE_EXECUTOR(bm25, T, wand_data_compressed)                  \
    PISA_MAXSCORE_EXECUTOR(dph, T, wand_data_compressed)                   \
    PISA_MAXSCORE_EXECUTOR(pl2, T, wand_data_compressed)                   \
    PISA_MAXSCORE_EXECUTOR(qld, T, wand_data_compressed)
/**/
BOOST_PP_SEQ_FOR_EACH(LOOP_BODY, _, PISA_INDEX_TYPES);
#undef LOOP_BODY
#undef PISA_MAXSCORE_EXECUTOR

} // namespace pisa
