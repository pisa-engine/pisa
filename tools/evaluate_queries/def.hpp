#include "block_freq_index.hpp"
#include "cursor/scored_cursor.hpp"
#include "freq_index.hpp"
#include "index_types.hpp"
#include "query/queries.hpp"
#include "scorer/bm25.hpp"
#include "scorer/dph.hpp"
#include "scorer/pl2.hpp"
#include "scorer/qld.hpp"

namespace pisa {

template <typename Index, typename Wand, typename Scorer>
using QueryLoop = std::function<std::vector<ResultVector>(
    Index const &, Wand const &, Scorer, std::vector<Query> const &, int)>;

template <typename Index, typename Algorithm, typename Wand, typename Scorer>
auto query_loop(Index const &, Wand const &, Scorer, std::vector<Query> const &, int k)
    -> std::vector<ResultVector>;

} // namespace pisa
