#include <iostream>
#include <taily.hpp>

#include "app.hpp"
#include "pisa/taily_stats.hpp"

namespace pisa {

void estimate_taily_thresholds(pisa::TailyThresholds const& args)
{
    auto stats = pisa::TailyStats::from_mapped(args.stats());
    for (auto const& query: args.queries()) {
        auto threshold = taily::estimate_cutoff(stats.query_stats(query), args.k());
        std::cout << threshold << '\n';
    }
}

}  // namespace pisa
