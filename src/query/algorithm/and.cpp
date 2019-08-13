#include "index_types.hpp"
#include "macro.hpp"
#include "query/algorithm/and_query.hpp"

namespace pisa {

#define LOOP_BODY(R, DATA, INDEX)                                                     \
    template std::vector<uint64_t> and_query::                                        \
    operator()<typename BOOST_PP_CAT(INDEX, _index)::document_enumerator>(            \
        gsl::span<typename BOOST_PP_CAT(INDEX, _index)::document_enumerator> cursors, \
        uint64_t max_docid) const;                                                    \
    template QueryExecutor and_executor(BOOST_PP_CAT(INDEX, _index) const &);
/**/
BOOST_PP_SEQ_FOR_EACH(LOOP_BODY, _, PISA_INDEX_TYPES);
#undef LOOP_BODY

} // namespace pisa
