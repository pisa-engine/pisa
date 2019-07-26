#pragma once

#include <string>

#include "bm25.hpp"
#include "dph.hpp"
#include "index_scorer.hpp"
#include "pl2.hpp"
#include "qld.hpp"
#include "spdlog/spdlog.h"

namespace pisa {

template <typename WandData, typename Func>
void with_scorer(std::string const &scorer_name, WandData const &wdata, Func func)
{
    if (scorer_name == "bm25") {
        func(bm25<WandData>(wdata));
    } else if (scorer_name == "qld") {
        func(qld<WandData>(wdata));
    } else if (scorer_name == "pl2") {
        func(pl2<WandData>(wdata));
    } else if (scorer_name == "dph") {
        func(dph<WandData>(wdata));
    } else {
        spdlog::error("Unknown scorer {}", scorer_name);
        std::abort();
    }
}

} // namespace pisa
