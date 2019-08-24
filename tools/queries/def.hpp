#include <chrono>

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
using QueryBenchmarkLoop = std::function<std::vector<std::chrono::microseconds>(
    Index const &, Wand const &, Scorer, std::vector<Query> const &, int, int)>;

template <typename Index, typename Algorithm, typename Wand, typename Scorer>
auto query_benchmark_loop(
    Index const &, Wand const &, Scorer, std::vector<Query> const &, int k, int runs)
    -> std::vector<std::chrono::microseconds>;

} // namespace pisa
